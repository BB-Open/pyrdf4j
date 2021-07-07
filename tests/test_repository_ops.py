from datenadler_rdf4j.api import triple_store
from requests.auth import HTTPBasicAuth
from http import HTTPStatus
from unittest import TestCase

from tests.constants import AUTH


class TestRepositoryCreate(TestCase):

    def setUp(self):
        for actor in AUTH:
            repo_id = 'test_{}'.format(actor)

            triple_store.rest_drop_repository(
                repo_id,
                auth=AUTH['admin']
            )

    def test_create_repository(self):
        EXPECT = {
            'viewer': None,
            'editor': None,
            'admin': HTTPStatus.NO_CONTENT,
          }

        for actor, expected in EXPECT.items():
            with self.subTest(actor=actor, expect=expected):
                repo_id = 'test_{}'.format(actor)
                response = triple_store.rest_create_repository(
                    repo_id,
                    'test_label_{}'.format(actor),
                    auth=AUTH[actor]
                )
                if expected is not None:
                    assert response.status_code == expected
                else:
                    assert response.status_code >= 400


class TestRepositoryDrop(TestCase):

    def setUp(self):
        for actor in AUTH:
            repo_id = 'test_{}'.format(actor)

            triple_store.rest_create_repository(
                repo_id,
                repo_id,
                auth=AUTH['admin']
            )

    def test_drop_repository(self):
        EXPECT = {
            'viewer': None,
            'editor': None,
            'admin': HTTPStatus.NO_CONTENT,
          }

        for actor, expected in EXPECT.items():
            with self.subTest(actor=actor, expect=expected):
                repo_id = 'test_{}'.format(actor)
                response = triple_store.rest_drop_repository(
                    repo_id,
                    auth=AUTH[actor]
                )
                if expected is not None:
                    assert response.status_code == expected
                else:
                    assert response.status_code >= 400
