import pickle

import numpy as np
import pytest

from enbios2.analyse.util import ResultsSelector
from enbios2.base.experiment import Experiment

@pytest.fixture
def experiment() -> Experiment:
    return pickle.load(open("exp.pkl", "rb"))


def test_base(experiment):
    dt = ResultsSelector(experiment)
    bl_df = dt.compare_to_baseline(np.array([0.2,3520112319301133e-08, 7, 0.5]))
    pass