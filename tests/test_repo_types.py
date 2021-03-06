from pyrdf4j.api_graph import APIGraph
from pyrdf4j.errors import CreateRepositoryError
from unittest import TestCase

from pyrdf4j.rdf4j import RDF4J
from pyrdf4j.repo_types import REPO_TYPES
from tests.constants import AUTH, RDF4J_BASE_TEST

# The custom rules are no parameters,
# so there cannot set a good default value.
# So we expect these cases to raise.
EXPECT_RAISE = {
    'memory-customrule': CreateRepositoryError,
    'native-customrule': CreateRepositoryError,
}


class TestRepoTypes(TestCase):
    def setUp(self):
        self.rdf4j = RDF4J(rdf4j_base=RDF4J_BASE_TEST)
        # make sure, no repo exists because of failed or stopped test before
        self.tearDown()

    def test_repo_types(self):
        for repo_type in REPO_TYPES:
            repo_type_safe = repo_type.replace('-', '_')
            with self.subTest(repo_type=repo_type):
                if repo_type in EXPECT_RAISE:
                    with self.assertRaises(EXPECT_RAISE[repo_type]):
                        self.rdf4j.create_repository(repo_type_safe, repo_type, auth=AUTH['admin'])
                else:
                    self.rdf4j.create_repository(
                        repo_type_safe,
                        repo_type,
                        auth=AUTH['admin']
                    )
                    # Test overwrite
                    self.rdf4j.create_repository(
                        repo_type_safe,
                        repo_type,
                        auth=AUTH['admin'],
                        overwrite=True
                    )
                    # Test accept exisiting
                    self.rdf4j.create_repository(
                        repo_type_safe,
                        repo_type,
                        auth=AUTH['admin'],
                        accept_existing=True
                    )

    def tearDown(self):
        for repo_type in REPO_TYPES:
            repo_type_safe = repo_type.replace('-', '_')
            sparql_endpoint = self.rdf4j.drop_repository(
                repo_type_safe,
                accept_not_exist=True,
                auth=AUTH['admin']
            )


class TestRepoTypesGraph(TestRepoTypes):

    def setUp(self):
        self.rdf4j = RDF4J(rdf4j_base=RDF4J_BASE_TEST, api=APIGraph)
