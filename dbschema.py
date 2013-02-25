#!/usr/bin/env python
# -*- coding: utf-8 -*-

from sqlalchemy.schema import MetaData, Table, Column, ForeignKey
from sqlalchemy.types import INT, FLOAT, CHAR, VARCHAR

from settings import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWD


metadata = MetaData()

license = Table('license', metadata,
    Column('id', INT, primary_key=True),
    Column('name', VARCHAR, nullable=False),
)

source = Table('source', metadata,
    Column('id', INT, primary_key=True),
    Column('name', VARCHAR, nullable=False),
)

dataset = Table('dataset', metadata,
    Column('id', INT, primary_key=True),
    Column('name', VARCHAR, nullable=False),
)

language = Table('language', metadata,
    Column('id', INT, primary_key=True),
    Column('name', VARCHAR, nullable=False),
)

concept = Table('concept', metadata,
    Column('id', INT, primary_key=True),
    Column('uri', VARCHAR, nullable=False),
    Column('language_id', INT, ForeignKey(language.c.id)),
)

assertion = Table('assertion', metadata,
    Column('id', INT, primary_key=True),
    Column('start_id', INT, ForeignKey(concept.c.id)),
    Column('relation_id', INT, ForeignKey(concept.c.id)),
    Column('end_id', INT, ForeignKey(concept.c.id)),
)

source_file = Table('source_file', metadata,
    Column('id', INT, primary_key=True),
    Column('filename', VARCHAR, nullable=False),
)

raw_assertion = Table('raw_assertion', metadata,
    Column('id', INT, primary_key=True),
    Column('assertion_id', INT, ForeignKey(assertion.c.id)),
    Column('license_id', INT, ForeignKey(license.c.id)),
    Column('dataset_id', INT, ForeignKey(dataset.c.id)),
    Column('source_file_id', INT, ForeignKey(source_file.c.id)),
    Column('surface_text', VARCHAR, nullable=False),
    Column('uri', VARCHAR, nullable=False),
    Column('weight', FLOAT, nullable=True),
    Column('score', FLOAT, nullable=True),
)

assertion_source = Table('assertion_source', metadata,
    Column('id', INT, primary_key=True),
    Column('raw_assertion_id', INT, ForeignKey(raw_assertion.c.id)),
    Column('source_id', INT, ForeignKey(source.c.id)),
)


def _test_schema():
    from sqlalchemy import create_engine

    # engine = create_engine('sqlite:///:memory:', echo=True)
    engine = create_engine('postgresql+psycopg2://%s:%s@%s:%s/%s' % (
        DB_USER, DB_PASSWD, DB_HOST, DB_PORT, DB_NAME), 
            client_encoding='utf8')
    metadata.create_all(engine)

if __name__ == '__main__':
    _test_schema()
    
