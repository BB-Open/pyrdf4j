from pyrdf4j.rdf4j import triple_store
from http import HTTPStatus
from unittest import TestCase

from pyrdf4j.repo_types import repo_config_factory
from tests.constants import AUTH


class TestRepositoryCreate(TestCase):

    def setUp(self):
        for actor in AUTH:
            repo_id = 'test_{}'.format(actor)

            triple_store.drop_repository(
                repo_id,
                auth=AUTH['admin'],
                accept_not_exist=True,
            )

    def tearDown(self) :
        for actor in AUTH:
            repo_id = 'test_{}'.format(actor)
            sparql_endpoint = triple_store.drop_repository(repo_id, accept_not_exist=True, auth=AUTH['admin'])


    def test_create_repository(self):
        EXPECT = {
            'viewer': HTTPStatus.FORBIDDEN,
            'admin': HTTPStatus.NO_CONTENT,
          }

        for actor, expected in EXPECT.items():
            with self.subTest(actor=actor, expect=expected):
                repo_id = 'test_{}'.format(actor)
                repo_uri = triple_store.repo_id_to_uri(repo_id)

                repo_label = repo_id

                repo_config = repo_config_factory(
                    'memory',
                    repo_id=repo_id,
                    repo_label=repo_label,
                    )


                response = triple_store.rest_create_repository(
                    repo_uri,
                    repo_config,
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
            triple_store.create_repository(
                repo_id,
                auth=AUTH['admin'],
                overwrite=True,
            )

    def tearDown(self) :
        for actor in AUTH:
            repo_id = 'test_{}'.format(actor)
            sparql_endpoint = triple_store.drop_repository(repo_id, accept_not_exist=True, auth=AUTH['admin'])


    def test_drop_repository(self):
        EXPECT = {
            'viewer': HTTPStatus.FORBIDDEN,
            'admin': HTTPStatus.NO_CONTENT,
          }

        for actor, expected in EXPECT.items():
            with self.subTest(actor=actor, expect=expected):
                repo_id = 'test_{}'.format(actor)
                repo_uri = triple_store.repo_id_to_uri(repo_id)

                response = triple_store.rest_drop_repository(
                    repo_uri,
                    auth=AUTH[actor]
                )
                if expected is not None:
                    assert response.status_code == expected
                else:
                    assert response.status_code >= 400
