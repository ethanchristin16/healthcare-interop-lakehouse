import pytest

from hc_lakehouse import synthetic


@pytest.fixture(scope="session")
def landing(tmp_path_factory):
    path = tmp_path_factory.mktemp("landing")
    synthetic.generate(path, n_patients=25, seed=11)
    return path
