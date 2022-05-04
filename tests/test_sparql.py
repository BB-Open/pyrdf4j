from http import HTTPStatus
from unittest import TestCase

from pyrdf4j.api_graph import APIGraph
from pyrdf4j.rdf4j import RDF4J
from tests.constants import AUTH, RDF4J_BASE_TEST


class TestSPARQL(TestCase):

    def setUp(self):
        self.rdf4j = RDF4J(RDF4J_BASE_TEST)
        self.rdf4j.create_repository('test_sparql', auth=AUTH['admin'], accept_existing=True)
        self.rdf4j.create_repository('test_sparql3', auth=AUTH['admin'], accept_existing=True)

        response = self.rdf4j.bulk_load_from_uri(
            'test_sparql',
            'https://opendata.potsdam.de/api/v2/catalog/exports/ttl',
            'application/x-turtle',
            auth=AUTH['admin']
        )
        assert response.status_code == HTTPStatus.OK

    def tearDown(self):
        sparql_endpoint = self.rdf4j.drop_repository(
            'test_sparql',
            auth=AUTH['admin'],
            accept_not_exist=True
        )
        sparql_endpoint3 = self.rdf4j.drop_repository(
            'test_sparql3',
            auth=AUTH['admin'],
            accept_not_exist=True
        )
        sparql_endpoint2 = self.rdf4j.drop_repository(
            'test_sparql2',
            auth=AUTH['admin'],
            accept_not_exist=True
        )

    def test_select_all(self):
        QUERY = "CONSTRUCT {?s ?o ?p} WHERE {?s ?o ?p}"
        response = self.rdf4j.get_triple_data_from_query(
            'test_sparql',
            QUERY,
            auth=AUTH['viewer'],
        )

        self.assertTrue('xml' in response.decode('utf8'))
        self.assertTrue(len(response.decode('utf8')) > 100)

    def test_copy_data(self):
        response = self.rdf4j.move_data_between_repositorys(
            'test_sparql2',
            'test_sparql',
            auth=AUTH['admin']
        )
        self.assertTrue(response.status_code == HTTPStatus.OK)
        QUERY = "CONSTRUCT {?s ?o ?p} WHERE {?s ?o ?p}"
        response = self.rdf4j.get_triple_data_from_query(
            'test_sparql2',
            QUERY,
            auth=AUTH['viewer'],
        )

        self.assertTrue('xml' in response.decode('utf8'))
        self.assertTrue(len(response.decode('utf8')) > 100)

    def test_query_ids(self):
        QUERY = """SELECT DISTINCT ?s
       WHERE {
          ?s a <http://www.w3.org/ns/dcat#Catalog>
       }"""
        response = self.rdf4j.query_repository('test_sparql', QUERY, auth=AUTH['admin'])

        self.assertTrue('s' in response['head']['vars'])
        self.assertTrue(len(response['results']['bindings']) >= 1)
        self.assertTrue('s' in response['results']['bindings'][0])

    def test_query_empty(self):
        QUERY = """SELECT DISTINCT ?s
       WHERE {
          ?s a <http://www.w3.org/ns/dcat#Catalog2>
       }"""
        response = self.rdf4j.query_repository('test_sparql', QUERY, auth=AUTH['admin'])

        self.assertTrue('s' in response['head']['vars'])
        self.assertTrue(len(response['results']['bindings']) == 0)

    def test_add(self):
        DATA = '<http://www.opendatasoft.com> ' \
               '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ' \
               '<http://xmlns.com/foaf/0.1/Agent> .'
        self.rdf4j.add_data_to_repo('test_sparql3', DATA, 'text/turtle', auth=AUTH['admin'])

        QUERY = """SELECT DISTINCT ?s
           WHERE {
            ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Agent>
           }"""
        response = self.rdf4j.query_repository('test_sparql3', QUERY, auth=AUTH['admin'])

        self.assertTrue('s' in response['head']['vars'])
        self.assertTrue(len(response['results']['bindings']) >= 1)
        self.assertTrue('s' in response['results']['bindings'][0])


class TestSPARQLGraph(TestSPARQL):

    def setUp(self):
        self.rdf4j = RDF4J(RDF4J_BASE_TEST, api=APIGraph)
        self.rdf4j.create_repository('test_sparql', auth=AUTH['admin'], accept_existing=True)
        self.rdf4j.create_repository('test_sparql3', auth=AUTH['admin'], accept_existing=True)

        response = self.rdf4j.bulk_load_from_uri(
            'test_sparql',
            'https://opendata.potsdam.de/api/v2/catalog/exports/ttl',
            'application/x-turtle',
            auth=AUTH['admin']
        )
        assert response.status_code == HTTPStatus.NO_CONTENT

    def test_copy_data(self):
        response = self.rdf4j.move_data_between_repositorys(
            'test_sparql2',
            'test_sparql',
            auth=AUTH['admin']
        )
        self.assertTrue(response.status_code == HTTPStatus.NO_CONTENT)
        QUERY = "CONSTRUCT {?s ?o ?p} WHERE {?s ?o ?p}"
        response = self.rdf4j.get_triple_data_from_query(
            'test_sparql2',
            QUERY,
            auth=AUTH['viewer'],
        )

        self.assertTrue('xml' in response.decode('utf8'))
        self.assertTrue(len(response.decode('utf8')) > 100)
