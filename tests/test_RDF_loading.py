from datenadler_rdf4j.api import triple_store
from datenadler_rdf4j.constants import RDF4J_BASE, ADMIN_USER, ADMIN_PASS, VIEWER_PASS, VIEWER_USER, EDITOR_USER, \
    EDITOR_PASS
from http import HTTPStatus

from unittest import TestCase

from datenadler_rdf4j.errors import TripleStoreCreateRepositoryAlreadyExists


class TestRepositoryAuth(TestCase):

    def setUp(self):
        try:
            triple_store.rest_drop_repository('test5')
        except :
            pass

    def test_bulk_load(self):

        sparql_endpoint = triple_store.create_repository('test5')
        response = triple_store.rest_bulk_load_from_uri(
            triple_store.repo_id_to_uri('test5'),
            'https://opendata.potsdam.de/api/v2/catalog/exports/ttl',
            'application/x-turtle',
        )

        assert response.status_code == HTTPStatus.OK