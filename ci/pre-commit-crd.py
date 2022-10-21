#!/usr/bin/env python
import os
import pathlib
import tempfile
import subprocess
import sys
import shutil

ROOT_DIR = pathlib.Path(__file__).parent.parent.absolute()


def install_deps(version):
    if not shutil.which("k8s-crd-resolver"):
        subprocess.run(
            [
                "pip",
                "install",
                f"git+https://github.com/elemental-lf/k8s-crd-resolver@{version}",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )


def run_generate(crd_path, patch_path, temp_path):
    subprocess.run(
        ["k8s-crd-resolver", "-r", "-j", patch_path, crd_path, temp_path], check=True
    )


def run_action(changed_file, temp_dir, crd_path, output_paths):
    # There should only be 2 or 3 values, but it depends if the dev committed
    # changes to "daskcluster.yaml" or "daskcluster.patch.yaml"
    # Either way, we only care about the first value in the output below
    file_name_components = os.path.basename(changed_file).split(".")
    file_name = file_name_components[0]

    output_file = os.path.join(temp_dir.name, f"{file_name}.yaml")
    run_generate(
        os.path.join(crd_path, f"{file_name}.yaml"),
        os.path.join(crd_path, f"{file_name}.patch.yaml"),
        output_file,
    )

    shutil.copyfile(output_file, f"{output_paths[0]}/{file_name}.yaml")
    shutil.copyfile(output_file, f"{output_paths[1]}/{file_name}.yaml")


def main(version, *args):
    install_deps(version)
    # Given a list of files that have been changed in a commit
    # We want to run the `k8s-crd-resolver` command and copy the relevant output files
    # and then check that nothing has changed

    output_paths = [
        os.path.join(
            ROOT_DIR, "dask_kubernetes", "operator", "deployment", "manifests"
        ),
        os.path.join(
            ROOT_DIR,
            "dask_kubernetes",
            "operator",
            "deployment",
            "helm",
            "dask-kubernetes-operator",
            "crds",
        ),
    ]

    temp_dir = tempfile.TemporaryDirectory()
    crd_path = os.path.join("dask_kubernetes", "operator", "customresources")

    for changed_file in args:

        if crd_path not in changed_file:
            # Precommit wont let us exclude files, so this will check if a file is inside the CRD path
            continue

        changed_file_name = os.path.basename(changed_file)
        if changed_file_name == "templates.yaml":
            # This is a special case - if we change the template file, we need re-render all of the charts
            # TODO: Not sure how to do this other than hard-code the template names
            run_action("daskcluster.yaml", temp_dir, crd_path, output_paths)
            run_action("daskworkergroup.yaml", temp_dir, crd_path, output_paths)
            run_action("daskjob.yaml", temp_dir, crd_path, output_paths)
            run_action("daskautoscaler.yaml", temp_dir, crd_path, output_paths)

        else:
            run_action(changed_file, temp_dir, crd_path, output_paths)


if __name__ == "__main__":
    main(*sys.argv)
