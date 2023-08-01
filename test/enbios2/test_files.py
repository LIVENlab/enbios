from enbios2.const import BASE_DATA_PATH
from enbios2.generic.files import ReadPath, ReadDataPath


def test_json_reader():
    json_data = ReadPath(BASE_DATA_PATH / "test_data/test_file_reader/a.json").read_data()
    assert json_data == {"a": 1}


def test_csv_reader():
    csv_data = ReadPath(BASE_DATA_PATH / "test_data/test_file_reader/a.csv").read_data()
    assert csv_data == [{'hello': 'a', 'value': '1'}]


def test_excel_reader():
    excel_data = ReadDataPath("test_data/test_file_reader/a.xlsx").read_data()
    print(excel_data)
    assert excel_data == {'sheet1': [('a', 1)],
                          'sheet2': [('ax', 'ay'), (1, 2)]}
