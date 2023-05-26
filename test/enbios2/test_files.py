from enbios2.const import BASE_DATA_PATH
from enbios2.generic.files import ReadPath, ReadDataPath


def test_reader():
    json_data = ReadDataPath("test/test_file_reader/a.json").read_data()
    assert json_data == {"a": 1}
    csv_data = ReadPath(BASE_DATA_PATH / "test/test_file_reader/a.csv").read_data()
    assert csv_data == [{'hello': 'a', 'value': '1'}]
    excel_data = ReadDataPath("test/test_file_reader/a.xlsx").read_data()
    assert excel_data == {'sheet1': [('a',), (1,)],
                          'Sheet2': [('ax', 'ay', None, None), (1, 2, None, None), (None, None, None, None),
                                     (None, None, None, None), (None, None, None, None), (None, None, None, None),
                                     (None, None, None, None), (None, None, None, None)]}
