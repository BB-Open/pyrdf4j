from pyrdf4j.rdf4j import RDF4J
from http import HTTPStatus

from unittest import TestCase

from pyrdf4j.errors import CreateRepositoryAlreadyExists
from tests.constants import AUTH, RDF4J_BASE_TEST


class TestRDFLoading(TestCase):

    def setUp(self):
        self.rdf4j = RDF4J(RDF4J_BASE_TEST) 
        
        self.rdf4j.create_repository('test_bulk_load', auth=AUTH['admin'])

    def tearDown(self) :
        sparql_endpoint = self.rdf4j.drop_repository('test_bulk_load', auth=AUTH['admin'])

    def test_bulk_load(self):
#        try:
#            self.rdf4j.create_repository('test_bulk_load', auth=AUTH['admin'])
#        except CreateRepositoryAlreadyExists:
#            pass
        response = self.rdf4j.bulk_load_from_uri(
            'test_bulk_load',
            'https://opendata.potsdam.de/api/v2/catalog/exports/ttl',
            'application/x-turtle',
            auth=AUTH['admin']
        )

        assert response.status_code == HTTPStatus.OK