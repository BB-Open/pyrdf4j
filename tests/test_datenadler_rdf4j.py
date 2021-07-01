from datenadler_rdf4j import __version__

from unittest import TestCase

from datenadler_rdf4j.api import tripel_store


def test_version():
    assert __version__ == '0.1.0'

class TestRepositoryAPI(TestCase):

    def test_create_repository(self):
        sparql_endpoint = tripel_store.create_repository('test')





class TestRepositoryAuth(TestCase):

    def testGet(self):
        pass

