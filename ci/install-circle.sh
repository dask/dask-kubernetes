set -xe

conda update -q conda
conda env create -f ci/environment-${PYTHON}.yml --name=${ENV_NAME}
conda env list
source activate ${ENV_NAME}
pip install --no-deps --quiet -e .
conda list ${ENV_NAME}
