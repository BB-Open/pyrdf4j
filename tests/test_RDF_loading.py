from requests.auth import HTTPBasicAuth

from pyrdf4j.rdf4j import triple_store
from pyrdf4j.constants import RDF4J_BASE, ADMIN_USER, ADMIN_PASS, VIEWER_PASS, VIEWER_USER, EDITOR_USER, \
    EDITOR_PASS
from http import HTTPStatus

from unittest import TestCase

from pyrdf4j.errors import TripleStoreCreateRepositoryAlreadyExists


class TestRDFLoading(TestCase):

    def setUp(self):
        try:
            triple_store.drop_repository('test5')
        except :
            pass

    def test_bulk_load(self):


        try:
            triple_store.create_repository('test5', auth=HTTPBasicAuth(ADMIN_USER, ADMIN_PASS))
        except TripleStoreCreateRepositoryAlreadyExists:
            pass
        response = triple_store.bulk_load_from_uri(
            'test5',
            'https://opendata.potsdam.de/api/v2/catalog/exports/ttl',
            'application/x-turtle',
            auth=HTTPBasicAuth(ADMIN_USER, ADMIN_PASS)
        )

        assert response.status_code == HTTPStatus.OK