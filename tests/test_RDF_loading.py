from pyrdf4j.api_graph import APIGraph
from pyrdf4j.rdf4j import RDF4J
from http import HTTPStatus

from unittest import TestCase

from pyrdf4j.errors import CreateRepositoryAlreadyExists
from tests.constants import AUTH, RDF4J_BASE_TEST


class TestRDFLoading(TestCase):

    def setUp(self):
        self.rdf4j = RDF4J(RDF4J_BASE_TEST)
        self.rdf4j.create_repository('test_bulk_load', auth=AUTH['admin'], overwrite=True)
        self.response_code_ok = HTTPStatus.OK

    def tearDown(self):
        sparql_endpoint = self.rdf4j.drop_repository('test_bulk_load', auth=AUTH['admin'], accept_not_exist=True)
        sparql_endpoint = self.rdf4j.drop_repository('test_bulk_load2', auth=AUTH['admin'], accept_not_exist=True)

    def test_bulk_load_repo_api(self):
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

        assert response.status_code == self.response_code_ok

    def test_bulk_load_replace(self):
        response = self.rdf4j.bulk_load_from_uri(
            'test_bulk_load',
            'https://opendata.potsdam.de/api/v2/catalog/exports/ttl',
            'application/x-turtle',
            auth=AUTH['admin'],
            clear_repository=True
        )

        assert response.status_code == self.response_code_ok

    def test_graph_from_uri(self):
        response = self.rdf4j.graph_from_uri(
            'test_bulk_load2',
            'https://opendata.potsdam.de/api/v2/catalog/exports/ttl',
            'application/x-turtle',
            auth=AUTH['admin'],
        )
        assert response.status_code == self.response_code_ok

class TestRDFLoadingGraph(TestRDFLoading):

    def setUp(self):
        self.rdf4j = RDF4J(RDF4J_BASE_TEST, api=APIGraph)
        self.rdf4j.create_repository('test_bulk_load', auth=AUTH['admin'], overwrite=True)
        self.response_code_ok = HTTPStatus.NO_CONTENT
