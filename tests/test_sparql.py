from pyrdf4j.api_graph import APIGraph
from pyrdf4j.api_repo import APIRepo
from pyrdf4j.rdf4j import RDF4J
from http import HTTPStatus
from unittest import TestCase

from pyrdf4j.errors import CreateRepositoryAlreadyExists
from tests.constants import AUTH, RDF4J_BASE_TEST


class TestSPARQL(TestCase):

    def setUp(self):
        self.rdf4j = RDF4J(RDF4J_BASE_TEST)
        self.rdf4j.create_repository('test_sparql', auth=AUTH['admin'], accept_existing=True)

        response = self.rdf4j.bulk_load_from_uri(
            'test_sparql',
            'https://opendata.potsdam.de/api/v2/catalog/exports/ttl',
            'application/x-turtle',
            auth=AUTH['admin']
        )
        assert response.status_code == HTTPStatus.OK

    def tearDown(self) :
        sparql_endpoint = self.rdf4j.drop_repository('test_sparql', auth=AUTH['admin'])

    def test_select_all(self):
        QUERY = """
        SELECT ?s ?o ?p WHERE {?s ?o ?p}
        """
#        for api in [APIGraph, APIRepo]:
        for api in [APIGraph]:
            with self.subTest(api=api.__name__):
                db = RDF4J(api=api, rdf4j_base=RDF4J_BASE_TEST)
                try:
                    response = db.get_triple_data_from_query(
                        'test_sparql',
                        QUERY,
                        auth=AUTH['viewer'],
                    )
                except Exception:
                    pass