from pyrdf4j.rdf4j import triple_store
from http import HTTPStatus

from unittest import TestCase

from pyrdf4j.errors import TripleStoreCreateRepositoryAlreadyExists
from tests.constants import AUTH


class TestRDFLoading(TestCase):

    def setUp(self):
        try:
            triple_store.drop_repository('test5')
        except :
            pass

    def tearDown(self) :
        sparql_endpoint = triple_store.drop_repository('test5', auth=AUTH['admin'])


    def test_bulk_load(self):


        try:
            triple_store.create_repository('test5', auth=AUTH['admin'])
        except TripleStoreCreateRepositoryAlreadyExists:
            pass
        response = triple_store.bulk_load_from_uri(
            'test5',
            'https://opendata.potsdam.de/api/v2/catalog/exports/ttl',
            'application/x-turtle',
            auth=AUTH['admin']
        )

        assert response.status_code == HTTPStatus.OK