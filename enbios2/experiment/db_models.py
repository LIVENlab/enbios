from peewee import UUIDField, Model, TextField, FloatField

from sqlite import TupleJSONField


class ActivityLCI(Model):
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

