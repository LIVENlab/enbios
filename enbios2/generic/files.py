import json
from csv import DictReader
from pathlib import Path

import openpyxl

from enbios2.const import BASE_DATA_PATH


class ReadPath(Path):
    """
    Checks on instantiation that the file exists
    """
    _flavour = Path('.')._flavour

    def __new__(cls, *args, **kwargs):
        instance = super().__new__(cls, *args, **kwargs)
        if not instance.exists():
            raise FileNotFoundError(f"File {instance} does not exist")
        return instance

    def read_data(self):
        """
        Read data from file. Formats supported: json, csv, excel
        - json is read straight into a dict
        - csv is read into a DictReader object
        - excel is read into a dict of sheet names and lists of rows

        :return:
        """
        if self.suffix == ".json":
            return json.loads(self.read_text(encoding="utf-8"))
        elif self.suffix == ".csv":
            return list(DictReader(self.open(encoding="utf-8")))
        # excel
        elif self.suffix == ".xlsx":
            workbook = openpyxl.load_workbook(self, read_only=True, data_only=True)
            return {sheet.title: list(sheet.values) for sheet in workbook.worksheets}


class ReadDataPath(ReadPath):
    def __new__(cls, *args, **kwargs):
        instance = super().__new__(cls, BASE_DATA_PATH, *args, **kwargs)
        if not instance.exists():
            raise FileNotFoundError(f"File {instance} does not exist")
        return instance
