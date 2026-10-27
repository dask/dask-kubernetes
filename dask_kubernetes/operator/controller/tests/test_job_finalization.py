import asyncio
import logging
from unittest.mock import AsyncMock, patch

import kr8s
import pytest

from dask_kubernetes.operator.controller.controller import (
    handle_runner_status_change_failed,
    handle_runner_status_change_succeeded,
)


@pytest.mark.parametrize(
    ("handler", "expected_status"),
    [
        (handle_runner_status_change_succeeded, "Successful"),
        (handle_runner_status_change_failed, "Failed"),
    ],
)
@pytest.mark.parametrize("missing_at", ["lookup", "delete"])
def test_finalization_retries_when_cluster_is_already_gone(
    handler, expected_status, missing_at
):
    cluster = AsyncMock()
    cluster_get = AsyncMock(return_value=cluster)
    job = AsyncMock()
    # The first status patch fails after the cluster has been deleted.
    job.patch.side_effect = [RuntimeError("temporary API failure"), None]
    if missing_at == "lookup":
        cluster_get.side_effect = [cluster, kr8s.NotFoundError("gone")]
    else:
        cluster.delete.side_effect = [None, kr8s.NotFoundError("gone")]

    meta = {"labels": {"dask.org/cluster-name": "my-job"}}
    with (
        patch(
            "dask_kubernetes.operator.controller.controller.DaskCluster.get",
            cluster_get,
        ),
        patch(
            "dask_kubernetes.operator.controller.controller.DaskJob.get",
            AsyncMock(return_value=job),
        ),
    ):
        with pytest.raises(RuntimeError, match="temporary API failure"):
            asyncio.run(handler(meta, "test-ns", logging.getLogger(__name__)))
        asyncio.run(handler(meta, "test-ns", logging.getLogger(__name__)))

    assert job.patch.await_count == 2
    payload = job.patch.await_args.args[0]
    assert payload["status"]["jobStatus"] == expected_status
    assert job.patch.await_args.kwargs["subresource"] == "status"


@pytest.mark.parametrize(
    "handler",
    [handle_runner_status_change_succeeded, handle_runner_status_change_failed],
)
@pytest.mark.parametrize("failure_at", ["lookup", "delete"])
def test_finalization_propagates_unexpected_cluster_errors(handler, failure_at):
    cluster = AsyncMock()
    cluster_get = AsyncMock(return_value=cluster)
    if failure_at == "lookup":
        cluster_get.side_effect = RuntimeError("API unavailable")
    else:
        cluster.delete.side_effect = RuntimeError("API unavailable")
    job_get = AsyncMock()
    with (
        patch(
            "dask_kubernetes.operator.controller.controller.DaskCluster.get",
            cluster_get,
        ),
        patch("dask_kubernetes.operator.controller.controller.DaskJob.get", job_get),
        pytest.raises(RuntimeError, match="API unavailable"),
    ):
        asyncio.run(
            handler(
                {"labels": {"dask.org/cluster-name": "my-job"}},
                "test-ns",
                logging.getLogger(__name__),
            )
        )
    job_get.assert_not_awaited()
