from enbios2.base.experiment import Experiment
from enbios2.generic.files import DataPath
input = DataPath("C:/Users/Administrator/PycharmProjects/enbios2/projects/seed/Data/enbios_input_3.json")
input_data = input.read_data()
exp = Experiment(input_data)
exp.run()
