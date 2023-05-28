from enbios2.generic.files import ReadDataPath


def get_ecoinvent_geo_data(file_path: ReadDataPath) -> list[dict]:
    """
    :param file_path:
    :return:
    """
    return [
        {"name": geo['name']['#text'],
         "code": geo['shortname']['#text'],
         "longitude": geo.get('@longitude'),
         "latitude": geo.get('@latitude')}
        for geo in file_path.read_data()['validGeographies']['geography']]


def geo_code2name(ecoinvent_geo_data: list[dict]) -> dict[str, str]:
    return {geo['code']: geo['name'] for geo in ecoinvent_geo_data}


# a = get_ecoinvent_geo_data(ReadDataPath("ecoinvent/ecoinvent 3.9.1_cutoff_ecoSpold02/MasterData/Geographies.xml"))
# code2name

# TODO next step... geoquery...
# from peewee import *
# from playhouse.sqlite_ext import SqliteExtDatabase
#
# # Create a database instance
# db = SqliteExtDatabase('my_database.sqlite', extensions=['mod_spatialite'])
#
# class BaseModel(Model):
#     class Meta:
#         database = db
#
# class TestGeom(BaseModel):
#     id = AutoField()
#     name = CharField(unique=True)
#     geom = TextField()  # Store WKT here
#
# # Connect to the database
# db.connect()
#
# # Create tables
# db.create_tables([TestGeom])
#
# # Insert data into the table
# TestGeom.create(name='Test Point', geom='POINT(10 20)')
#
# # Perform a spatial query
# query = (TestGeom
#          .select()
#          .where(fn.ST_Distance(
#              fn.GeomFromText(TestGeom.geom, 4326),
#              fn.GeomFromText('POINT(10 20)', 4326)
#          ) < 10))
#
# # Fetch and print the results
# for row in query:
#     print(row.name, row.geom)
#
# # Close the connection
# db.close()
