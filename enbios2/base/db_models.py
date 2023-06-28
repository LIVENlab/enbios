from pathlib import Path
from typing import Union, Optional

from peewee import Model, TextField, FloatField, BooleanField, SqliteDatabase, ForeignKeyField
from playhouse.shortcuts import model_to_dict
from playhouse.sqlite_ext import FTSModel, JSONField

from enbios2.base.db_fields import TupleJSONField, PathField
from enbios2.const import MAIN_DATABASE_PATH


class MainDatabase(Model):
    pass

    class Meta:
        database = SqliteDatabase(MAIN_DATABASE_PATH)


class EcoinventDataset(MainDatabase):
    """
    Ecoinvent datasets model
    """
    version = TextField()  # should have validation // see ecoinvent_consts
    system_model = TextField()  # cutoff, consequential, apos
    type = TextField(default="default")  # ecoinvent_dataset_types
    xlsx = BooleanField(default=False)
    directory: Path = PathField(null=True)  # Path typehint, so that static checker chills
    identity = TextField(unique=True)

    _V391 = "3.9.1"
    _SM_CUTOFF = "cutoff"
    _SM_CONSEQUENTIAL = "consequential"
    _SM_APOS = "apos"
    _valid_ecoinvent_versions = {"3.8", "3.9", "3.9.1"}
    _valid_ecoinvent_system_models = {"cutoff", "consequential", "apos"}
    _valid_ecoinvent_datatypes = {"default", "lci", "lcia"}

    @property
    def bw_project_index(self) -> Optional["BWProjectIndex"]:
        return self.bw_project_db.get_or_none()

    class Meta:
        table_name = "ecoinvent_dataset"

    def __init__(self, *args, **kwargs):
        super(EcoinventDataset, self).__init__(*args, **kwargs)
        self.identity = f"{self.system_model}_{self.version}_{self.type}{'_xlsx' if self.xlsx else ''}"

    def save(self, *args, **kwargs):
        check_fields = [("version", EcoinventDataset._valid_ecoinvent_versions),
                        ("system_model", EcoinventDataset._valid_ecoinvent_system_models),
                        ("type", EcoinventDataset._valid_ecoinvent_datatypes)]
        for field, valid_values in check_fields:
            if getattr(self, field) not in valid_values:
                raise ValueError(
                    f"EcoinventIndex entry '{field}' is not valid: {getattr(self, field)}. Must be of {valid_values}")
        super(EcoinventDataset, self).save(*args, **kwargs)

    @classmethod
    def identity_exists(cls, identity: Union["EcoinventDataset", str]) -> bool:
        """
        Check if the given identity exists in the database
        :param identity: identity string or EcoinventDataset instance
        :return: True, if exists
        """
        if isinstance(identity, EcoinventDataset):
            identity = identity.identity
        return cls.select().where(cls.identity == identity).exists()

    @property
    def dataset_path(self) -> Path:
        """
        get the definite path of the dataset (spold files) or Excel file
        :return: Path of 'datasets' within the folder or Excel file
        """
        if self.xlsx:
            return next(self.directory.glob("*.xlsx"))
        else:
            return self.directory / f"datasets"

    __repr__ = __str__ = lambda self: f"EcoinventDataset: {self.identity}"

    @staticmethod
    def dump_database(entries: Optional[list["EcoinventDataset"]] = None,
                      redact_dir: Optional[bool] = True) -> list[dict]:
        """
        Dump the database to a JSON file
        :return:
        """
        res = []
        for entry in entries if entries else EcoinventDataset.select():
            d = model_to_dict(entry)
            if redact_dir:
                d["directory"] = "***/" + entry.directory.name
            if bwp := entry.bw_project_db.get_or_none():
                d["bw_project"] = [bwp.project_name, bwp.database_name]
            res.append(d)
        return res


class EcoinventResolvedDataset(MainDatabase):
    """
    Metadata table for Ecoinvent databases
    """
    name = TextField()
    path = TextField()
    db_type = TextField()
    metadata = JSONField()
    ecoinvent_dataset = ForeignKeyField(EcoinventDataset, backref='resolved_dataset', unique=True)

    class Meta:
        table_name = "ecoinvent_resolved_dataset"


class BWProjectIndex(MainDatabase):
    project_name = TextField()
    database_name = TextField()
    ecoinvent_dataset = ForeignKeyField(EcoinventDataset, backref='bw_project_db', unique=True)

    class Meta:
        table_name = "bw_project_index"

    def __repr__(self):
        return f"BWProjectIndex: {self.project_name} - {self.database_name} ({self.ecoinvent_dataset})"

    def __str__(self):
        return self.__repr__()


class EcoinventDatabaseActivity(Model):
    """
    Main table for the 2 resolved Ecoinvent databases (LCI, LCIA)
    """
    code = TextField(unique=True)
    name = TextField(index=True)
    location = TextField()
    product = TextField()
    product_unit = TextField()
    amount = FloatField()
    data = TupleJSONField()

    class Meta:
        pass


class ExchangeInfo(Model):
    """
    Ecoinvent LCI Index
    """
    exchange = TextField()
    compartment = TextField()
    sub_compartment = TextField()
    unit = TextField()

    class Meta:
        pass


class ImpactInfo(Model):
    """
    Ecoinvent LCIA Index
    """
    method = TextField()
    category = TextField()
    indicator = TextField()
    unit = TextField()

    class Meta:
        pass


class BW_Activity(Model):
    """
    experimental FTS database
    """
    code = TextField()
    database = TextField()
    name = TextField()
    location = TextField(null=True)
    location_name = TextField(null=True)
    product = TextField(null=True)
    type = TextField()
    # from data
    # categories = TextField()
    comment = TextField(null=True)
    classification = TextField(null=True)
    # synonyms = TextField()
    unit = TextField(null=True)
    reference_product = TextField(null=True)


class FTS_BW_ActivitySimple(FTSModel):
    """
    experiment full text search database
    """
    name = TextField()
    product = TextField()
    synonyms = TextField()
    location_name = TextField()
    comment = TextField()
    content = TextField()
