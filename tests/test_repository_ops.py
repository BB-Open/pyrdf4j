from http import HTTPStatus
from unittest import TestCase

from pyrdf4j.api_graph import APIGraph
from pyrdf4j.api_repo import APIRepo
from pyrdf4j.rdf4j import RDF4J
from pyrdf4j.repo_types import repo_config_factory
from tests.constants import AUTH, RDF4J_BASE_TEST


class TestRepositoryCreate(TestCase):

    def setUp(self):
        self.rdf4j = RDF4J(rdf4j_base=RDF4J_BASE_TEST)
        for actor in AUTH:
            repo_id = 'test_{}'.format(actor)

            self.rdf4j.drop_repository(
                repo_id,
                auth=AUTH['admin'],
                accept_not_exist=True,
            )

    def tearDown(self):
        for actor in AUTH:
            repo_id = 'test_{}'.format(actor)
            sparql_endpoint = self.rdf4j.drop_repository(repo_id, accept_not_exist=True, auth=AUTH['admin'])

    def test_create_repository(self):
        EXPECT = {
            'viewer': HTTPStatus.FORBIDDEN,
            'admin': HTTPStatus.NO_CONTENT,
        }

        for actor, expected in EXPECT.items():
            with self.subTest(actor=actor, expect=expected):
                repo_id = 'test_{}'.format(actor)

                repo_label = repo_id

                repo_config = repo_config_factory(
                    'memory',
                    repo_id=repo_id,
                    repo_label=repo_label,
                )

                api = self.rdf4j.get_api(repo_id)

                response = api.create_repository(
                    repo_config,
                    auth=AUTH[actor]
                )
                if expected is not None:
                    assert response.status_code == expected
                else:
                    assert response.status_code >= 400


class TestRepositoryCreateGraph(TestRepositoryCreate):

    def setUp(self):
        self.rdf4j = RDF4J(rdf4j_base=RDF4J_BASE_TEST, api=APIGraph)
        for actor in AUTH:
            repo_id = 'test_{}'.format(actor)

            self.rdf4j.drop_repository(
                repo_id,
                auth=AUTH['admin'],
                accept_not_exist=True,
            )


class TestRepositoryDrop(TestCase):
    def setUp(self):
        self.rdf4j = RDF4J(rdf4j_base=RDF4J_BASE_TEST)
        for actor in AUTH:
            repo_id = 'test_{}'.format(actor)
            self.rdf4j.create_repository(
                repo_id,
                auth=AUTH['admin'],
                overwrite=True,
            )

    def tearDown(self):
        for actor in AUTH:
            repo_id = 'test_{}'.format(actor)
            sparql_endpoint = self.rdf4j.drop_repository(repo_id, accept_not_exist=True, auth=AUTH['admin'])

    def test_drop_repository(self):
        EXPECT = {
            'viewer': HTTPStatus.FORBIDDEN,
            'admin': HTTPStatus.NO_CONTENT,
        }

        for actor, expected in EXPECT.items():
            with self.subTest(actor=actor, expect=expected):
                repo_id = 'test_{}'.format(actor)

                api = self.rdf4j.get_api(repo_id)

                response = api.drop_repository(
                    auth=AUTH[actor]
                )
                if expected is not None:
                    assert response.status_code == expected
                else:
                    assert response.status_code >= 400


class TestRepositoryDropGraph(TestRepositoryDrop):
    def setUp(self):
        self.rdf4j = RDF4J(rdf4j_base=RDF4J_BASE_TEST, api=APIGraph)
        for actor in AUTH:
            repo_id = 'test_{}'.format(actor)
            self.rdf4j.create_repository(
                repo_id,
                auth=AUTH['admin'],
                overwrite=True,
            )


class TestRepositoryCreateByAPIRepo(TestCase):

    def setUp(self):
        self.api = APIRepo
        self.rdf4j = RDF4J(rdf4j_base=RDF4J_BASE_TEST, api=self.api)
        self.server = self.rdf4j.server
        for actor in AUTH:
            repo_id = 'test_{}'.format(actor)

            self.rdf4j.drop_repository(
                repo_id,
                auth=AUTH['admin'],
                accept_not_exist=True,
            )

    def tearDown(self):
        for actor in AUTH:
            repo_id = 'test_{}'.format(actor)
            sparql_endpoint = self.rdf4j.drop_repository(repo_id, accept_not_exist=True, auth=AUTH['admin'])

    def test_create_repository(self):
        EXPECT = {
            'viewer': HTTPStatus.FORBIDDEN,
            'admin': HTTPStatus.NO_CONTENT,
        }

        for actor, expected in EXPECT.items():
            with self.subTest(actor=actor, expect=expected):
                repo_id = 'test_{}'.format(actor)

                repo_label = repo_id

                repo_config = repo_config_factory(
                    'memory',
                    repo_id=repo_id,
                    repo_label=repo_label,
                )

                api_instance, response = self.api.create(self.server, repo_id, repo_config, auth=AUTH[actor])

                self.assertIsInstance(api_instance, self.api)

                if expected is not None:
                    assert response.status_code == expected
                else:
                    assert response.status_code >= 400


class TestRepositoryCreateByAPIGraph(TestRepositoryCreateByAPIRepo):

    def setUp(self):
        self.api = APIGraph
        self.rdf4j = RDF4J(rdf4j_base=RDF4J_BASE_TEST, api=self.api)
        self.server = self.rdf4j.server
        for actor in AUTH:
            repo_id = 'test_{}'.format(actor)

            self.rdf4j.drop_repository(
                repo_id,
                auth=AUTH['admin'],
                accept_not_exist=True,
            )