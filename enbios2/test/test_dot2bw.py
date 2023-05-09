import pytest

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
    bd.Database("graphDB").register()
    dot2bw = Dot2BW("graphDB", "dot/dot_example.dot")
    print(dot2bw.graph_diff)
    dot2bw = Dot2BW("graphDB", "dot/dot_example.dot")
    print(dot2bw.graph_diff)
    print("**")
    #
    #
    # dot2bw.define_db_with_graph(graph)
    #
    # #
    # method = ('CML 2001 (superseded)', 'climate change', 'GWP 20a')
    # method_data = bd.Method(method)
    # # print(list(dot2bw.database))
    # a = dot2bw.database.get(name="a")
    # #
    # list(a.biosphere())
    # print(a.lca(method=method, amount=1).score)
    # print("+**")

