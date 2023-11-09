import pickle

import numpy as np
import pytest

from enbios.base.result_select import ResultsSelector
from enbios.base.experiment import Experiment
from enbios.generic.files import DataPath


# @pytest.fixture
# def experiment() -> Experiment:
#     return pickle.load(DataPath("test_data/exp.pkl").open("rb"))


# def test_base(experiment):
#     dt = ResultsSelector(experiment)
#     #bl_df = dt.compare_to_baseline(np.array([0.2,3520112319301133e-08, 7, 0.5]))
#     pass