from enbios.const import BASE_TEST_DATA_PATH
from enbios.generic.files import ReadPath


def test_json_reader():
    json_data = ReadPath(BASE_TEST_DATA_PATH / "test_file_reader/a.json").read_data()
    assert json_data == {"a": 1}


def test_csv_reader():
    csv_data = ReadPath(BASE_TEST_DATA_PATH / "test_file_reader/a.csv").read_data()
    assert csv_data == [{'hello': 'a', 'value': '1'}]


def test_excel_reader():
    excel_data = ReadPath(BASE_TEST_DATA_PATH / "test_file_reader/a.xlsx").read_data()
    print(excel_data)
    assert excel_data == {'sheet1': [('a', 1)],
                          'sheet2': [('ax', 'ay'), (1, 2)]}
