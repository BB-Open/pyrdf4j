from pyrdf4j.errors import TripleStoreCreateRepositoryError
from pyrdf4j.rdf4j import triple_store
from unittest import TestCase

from pyrdf4j.repo_types import REPO_TYPES
from tests.constants import AUTH

# The custom rules are no parameters, so there cannot set a good default value. So we expect these cases to raise.
EXPECT_RAISE = {
    'memory-customrule': TripleStoreCreateRepositoryError,
    'native-customrule': TripleStoreCreateRepositoryError,
}


class TestRepoTypes(TestCase):

    def test_repo_types(self):
        for repo_type in REPO_TYPES:
            repo_type_safe =  repo_type.replace('-', '_')
            with self.subTest(repo_type=repo_type):
                if repo_type in EXPECT_RAISE:
                    with self.assertRaises(EXPECT_RAISE[repo_type]):
                        triple_store.create_repository(repo_type_safe, repo_type, auth=AUTH['admin'])
                else:
                    triple_store.create_repository(repo_type_safe, repo_type, auth=AUTH['admin']),

    def tearDown(self) :
        for repo_type in REPO_TYPES:
            repo_type_safe =  repo_type.replace('-', '_')
            try:
                sparql_endpoint = triple_store.drop_repository(
                    repo_type_safe,
                    accept_not_exist=True,
                    auth=AUTH['admin']
                )
            except:
                pass
