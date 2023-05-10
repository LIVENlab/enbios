import pytest
from bw2data.backends import Exchange
from bw2parameters import ParameterSet

from enbios2.const import BASE_DATA_PATH
from enbios2.experiment.dot2bw import Dot2BW, read_dot_file
import bw2data as bd


@pytest.fixture
def grapgDB_cleanup(request):
    def delete_database():
        bd.projects.set_current("ecoinvent_test")
        bd.Database("graphDB").delete()

    request.addfinalizer(delete_database)


def test_basic(grapgDB_cleanup):
    bd.projects.set_current("ecoinvent_test")
    import bw2io as bi
    # bi.bw2setup()
    # bd.Database("graphDB").register()
    # bd.parameters.new_activity_parameters({"some_param": 10}, "graphDB")
    dot2bw = Dot2BW("graphDB", "dot/dot_example.dot")
    print(dot2bw.graph_diff)
    # dot2bw = Dot2BW("graphDB", "dot/dot_example.dot")
    print(dot2bw.graph_diff)
    print("**")
    method = ('IPCC 2021',
              'climate change: fossil',
              'global temperature change potential (GTP100)')
    # method_data = bd.Method(method)
    # # print(list(dot2bw.database))
    a = dot2bw.database.get(name="a")
    # #
    print(list(a.biosphere())[0])
    print(a.lca(method=method, amount=1).score)
    # print("+**")
    # ParameterSet
    #
    #
    # parameters = {
    #     'Deep_Thought': {'amount': 42},
    #     'East_River_Creature': {'formula': '2 * Deep_Thought + 16'},
    #     'Elders_of_Krikkit': {'formula': 'sqrt(East_River_Creature)'},
    # }


