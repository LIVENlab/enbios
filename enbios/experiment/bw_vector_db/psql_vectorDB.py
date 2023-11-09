"""
This requires postgresql and the pgvector extension to be installed.
https://github.com/pgvector/pgvector
sqlalchemy, sqlmodel, psycopg2, and sentence_transformers
"""
from typing import List, Optional

from pgvector.sqlalchemy import Vector
from sqlalchemy import Column, Integer
from sqlalchemy.dialects.postgresql import ARRAY
from sqlmodel import SQLModel, Field, create_engine


class Document(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    content: str
    ext_ids:list[int] = Field([], sa_column=Column(ARRAY(Integer)))
    embedding: List[float] = Field(sa_column=Column(Vector(384)))
    norm_embedding: Optional[List[float]] = Field(sa_column=Column(Vector(384)))
    cluster : Optional[int]
    embedding_2d: Optional[List[float]] = Field(sa_column=Column(Vector(2)))
    norm_embedding_2d: Optional[List[float]] = Field(sa_column=Column(Vector(2)))


def init_engine(user: str, password: str, host: str, db_name: str):
    return create_engine(f'postgresql://{user}:{password}@{host}/{db_name}')


def create_database(db_name, user, password, host='localhost'):
    # connect to the default 'postgres' database
    engine = create_engine(f'postgresql://{user}:{password}@{host}/postgres')

    # get a raw DBAPI connection
    conn = engine.raw_connection()
    try:
        conn.set_isolation_level(0)  # AUTOCOMMIT
        conn.cursor().execute(f"CREATE DATABASE {db_name};")
    finally:
        conn.set_isolation_level(1)  # reset isolation level
        conn.close()

    # now connect to the new database and create extension
    # todo, following line required?
    engine = create_engine(f'postgresql://{user}:{password}@{host}/{db_name}')
    conn = engine.raw_connection()
    try:
        conn.set_isolation_level(0)  # AUTOCOMMIT
        conn.cursor().execute("CREATE EXTENSION IF NOT EXISTS vector;")
    finally:
        conn.set_isolation_level(1)  # reset isolation level
        conn.close()

def create_vector_db():
    create_database("activities_vectors", user_name, password)

def reset_vector_db():
    SQLModel.metadata.drop_all(engine)
    SQLModel.metadata.create_all(engine)


user_name = "postgres"
password = "bbe90ce51d87635c7354445e6887c73a"

engine = init_engine(user_name, password, "localhost", "activities_vectors")
SQLModel.metadata.create_all(engine)