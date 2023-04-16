import sqlalchemy


def drop_pg_database(sa_str, database_name):
    db_connection_string = sa_str
    data_engine = sqlalchemy.create_engine(db_connection_string, echo=False)
    conn = data_engine.connect()
    conn.execute("commit")
    try:
        conn.execute("drop database "+database_name)
    except:
        pass
    conn.close()
    data_engine.dispose()


def create_pg_database_engine(sa_str, database_name, recreate_db=False):
    if recreate_db:
        drop_pg_database(sa_str, database_name)
    db_connection_string = sa_str
    data_engine = sqlalchemy.create_engine(db_connection_string, echo=False)
    conn = data_engine.connect()
    conn.execute("commit")
    try:
        conn.execute("create database "+database_name)
    except:
        pass
    conn.close()
    data_engine.dispose()
    db_connection_string = sa_str+database_name
    return sqlalchemy.create_engine(db_connection_string, echo=False)


def create_monet_database_engine(sa_str, database_name):
    """
    docker run -d --name monetdb -P -p 50000:50000 -v /home/rnebot/DATOS/monetdb:/var/monetdb5/dbfarm monetdb/monetdb-r-docker
    import pymonetdb
    connection = pymonetdb.connect(username="monetdb", password="monetdb", hostname="localhost", database="db")

    MonetDB container
     - Create MonetDB "preparer", using "monet-r-docker" image
     - After container creation, create new DB
       - Implies
    Contenedor MONETDB
     - Crear preparador de instancia Monet, basado en Monet-R
       - Crear+iniciar contenedor
       - Crear BDD
    """
    db_connection_string = sa_str+database_name
    return sqlalchemy.create_engine(db_connection_string, echo=False)
