"""An asyncio shim for pykube-ng."""

import asyncio
import functools

from pykube.query import now, Table, Query as _Query, WatchQuery as _WatchQuery


class Query(_Query):
    async def _sync(self, func, *args, **kwargs):
        return await asyncio.get_event_loop().run_in_executor(
            None, functools.partial(func, *args, **kwargs)
        )

    async def execute(self, **kwargs):
        return await self._sync(super().execute, **kwargs)

    async def iterator(self):
        response = await self.execute()
        for obj in response.json().get("items") or []:
            yield self.api_obj_class(self.api, obj)

    def __aiter__(self):
        return self.iterator()

    async def get_by_name(self, name: str):
        return await self._sync(super().get_by_name, name=name)

    async def get(self, *args, **kwargs):
        return await self._sync(super().get, *args, **kwargs)

    async def get_or_none(self, *args, **kwargs):
        return await self._sync(super().get_or_none, *args, **kwargs)

    async def as_table(self) -> Table:
        response = await self.execute(
            headers={"Accept": "application/json;as=Table;v=v1beta1;g=meta.k8s.io"}
        )
        return Table(self.api_obj_class, response.json())

    def watch(self, since=None, *, params=None):
        if since is now:
            raise ValueError("now is not a supported since value in async version")
        return super().query(since, params=params)._clone(WatchQuery)

    @property
    def query_cache(self):
        raise NotImplementedError()

    @property
    def response(self):
        raise NotImplementedError()

    def __len__(self):
        raise TypeError("Cannot call len on async objects")


class WatchQuery(_WatchQuery):
    async def object_stream(self):
        object_stream = iter(super().object_stream())
        while True:
            try:
                yield await self._sync(next, object_stream)
            except StopIteration:
                break

    def __aiter__(self):
        return self.object_stream()
