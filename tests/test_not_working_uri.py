from unittest import TestCase

from pyrdf4j.errors import DataBaseNotReachable
from pyrdf4j.rdf4j import RDF4J
from tests.constants import AUTH

RDF4J_BASE_TEST = 'http://rdf4j-server/'


class TestDatabaseNotReachable(TestCase):

    def test_create_database(self):
        self.rdf4j = RDF4J(RDF4J_BASE_TEST)
        self.assertRaises(
            DataBaseNotReachable,
            self.rdf4j.create_repository,
            'test_sparql',
            auth=AUTH['admin'],
            accept_existing=True
        )


class TestDatabaseNotReachableGraph(TestDatabaseNotReachable):
    pass
