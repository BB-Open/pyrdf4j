from pyrdf4j.rdf4j import triple_store
from http import HTTPStatus
from unittest import TestCase

from pyrdf4j.errors import TripleStoreCreateRepositoryAlreadyExists
from tests.constants import AUTH


class TestSPARQL(TestCase):

    def setUp(self):
        try:
            triple_store.create_repository('test_sparql', auth=AUTH['admin'])
        except TripleStoreCreateRepositoryAlreadyExists:
            pass

        response = triple_store.bulk_load_from_uri(
            'test_sparql',
            'https://opendata.potsdam.de/api/v2/catalog/exports/ttl',
            'application/x-turtle',
            auth=AUTH['admin']
        )
        assert response.status_code == HTTPStatus.OK

    def tearDown(self) :
        sparql_endpoint = triple_store.drop_repository('test_sparql', auth=AUTH['admin'])


    def test_select_all(self):
        QUERY = """
        CLEAR DEFAULT
        """

        response = triple_store.get_triple_data_from_query('test_sparql', QUERY, 'application/rdf+xml')

        a=5