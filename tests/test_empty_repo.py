from http import HTTPStatus
from unittest import TestCase

from pyrdf4j.api_graph import APIGraph
from pyrdf4j.errors import URINotReachable
from pyrdf4j.rdf4j import RDF4J
from tests.constants import AUTH, RDF4J_BASE_TEST


class TestEmpty(TestCase):

    def setUp(self):
        self.rdf4j = RDF4J(RDF4J_BASE_TEST)
        self.rdf4j.create_repository(
            'test_bulk_load',
            auth=AUTH['admin'],
            overwrite=True,
            repo_type='native'
        )
        self.response_code_ok = HTTPStatus.OK

    def tearDown(self):
        sparql_endpoint = self.rdf4j.drop_repository(
            'test_bulk_load',
            auth=AUTH['admin'],
            accept_not_exist=True
        )

    def test_empty(self):
        response = self.rdf4j.bulk_load_from_uri(
            'test_bulk_load',
            'https://opendata.potsdam.de/api/v2/catalog/exports/ttl',
            'application/x-turtle',
            auth=AUTH['admin'],
        )

        response = self.rdf4j.empty_repository('test_bulk_load', auth=AUTH['admin'])

        QUERY = "CONSTRUCT {?s ?o ?p} WHERE {?s ?o ?p}"
        response = self.rdf4j.get_triple_data_from_query(
            'test_bulk_load',
            QUERY,
            auth=AUTH['viewer'],
        )
        self.assertTrue('Potsdam' not in response.decode('utf-8'))


class TestEmptyGraph(TestEmpty):

    def setUp(self):
        self.rdf4j = RDF4J(RDF4J_BASE_TEST, api=APIGraph)
        self.rdf4j.create_repository('test_bulk_load', auth=AUTH['admin'], overwrite=True)
        self.response_code_ok = HTTPStatus.NO_CONTENT
