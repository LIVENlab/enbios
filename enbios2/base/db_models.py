from peewee import Model, TextField, FloatField
from playhouse.sqlite_ext import FTSModel

from enbios2.base.db_fields import TupleJSONField


class EcoinventDatabaseActivity(Model):
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
    exchange = TextField()
    compartment = TextField()
    sub_compartment = TextField()
    unit = TextField()

    class Meta:
        pass


class ImpactInfo(Model):
    method = TextField()
    category = TextField()
    indicator = TextField()
    unit = TextField()

    class Meta:
        pass


class BW_Activity(Model):
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
    name = TextField()
    product = TextField()
    synonyms = TextField()
    location_name = TextField()
    comment = TextField()
    content = TextField()


# class BWProjectIndex(Model):
#     version: TextField()  # should have validation // see ecoinvent_consts
#     system_model: ecoinvent_system_models
#     type: ecoinvent_dataset_types = "default"
#     xlsx: bool = False
#     path: Optional[Path] = None

