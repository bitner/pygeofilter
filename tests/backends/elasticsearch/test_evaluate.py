from datetime import datetime

from elasticsearch_dsl import (
    Document, Date, Integer, Keyword, Text, Float, GeoPoint, GeoShape
)
from elasticsearch_dsl.connections import connections
import pytest

from pygeofilter.backends.elasticsearch import to_filter


class Record(Document):
    identifier = Text(analyzer='snowball', fields={'raw': Keyword()})
    float_attribute = Float()
    int_attribute = Integer()
    str_attribute = Text()
    datetime_attribute = Date()
    point = GeoPoint()
    geometry = GeoShape()

    class Index:
        name = 'records'
        settings = {
          "number_of_shards": 1,
        }


@pytest.fixture(scope="session")
def connection():
    # Define a default Elasticsearch client
    return connections.create_connection(hosts=['localhost'])


def evaluate(session, cql_expr, expected_ids):
    ast = parse(cql_expr)
    filters = to_filter(ast, FIELD_MAPPING)

    q = session.query(Record).join(RecordMeta).filter(filters)
    results = [row.identifier for row in q]

    assert expected_ids == type(expected_ids)(results)

