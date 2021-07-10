
from nose.tools import assert_equal

from pyrdf4j import __version__

from unittest import TestCase

from pyrdf4j.rdf4j import triple_store
from pyrdf4j.constants import RDF4J_BASE, ADMIN_USER, ADMIN_PASS, VIEWER_PASS, VIEWER_USER, EDITOR_USER, \
    EDITOR_PASS
from pyrdf4j.errors import TripleStoreCreateRepositoryAlreadyExists
from tests.constants import ACTORS, AUTH


def test_version():
    assert __version__ == '0.1.0'


PUT = triple_store.put
GET = triple_store.get
DELETE = triple_store.delete
POST = triple_store.post


repos_dir = 'repos_dir'
system_dir = 'system_dir'
repo = 'repo'

URIs = {
    repos_dir : RDF4J_BASE + 'repositories',
    system_dir : RDF4J_BASE + 'repositories/SYSTEM',
    repo : RDF4J_BASE + 'repositories/test',
}

QUERIES = {
    repo : {
        'params' :"""select ?s ?o ?p WHERE {?s ?p ?o}""",
        'headers' : {'content-type': 'application/sparql-query'},
    }
}

METHODS = {
    'put' : PUT,
    'get' : GET,
    'delete' : DELETE,
    'post' :POST
}

methods = METHODS.keys()

MATRIX = {
    repos_dir : {
        'admin' : {'get': 200,
                   'post': 500,
                   },
        'editor': {'get': 200,
                   'post': 500,
                   },
        'viewer': {'get': 200,
                   'post': 500,
                   }
    },
    system_dir: {
        'admin' : {'get': 400,
                   'post': 415,
                   },
    },
    repo: {
        'admin' : {'get': 200,
                   'post': 200,
                   'put': 409,
                   'delete': 204,
                   },
        'editor' : {'get': 200,
                   'post': 200,
                   'put': 409,
                   'delete': 204,
                   },
        'viewer' : {'get': 200,
                   'post': 200,
                   }
    },
}

PARAMS = []
for path in MATRIX:
    for actor in ACTORS:
        for method in methods:
            try:
                expected = MATRIX[path][actor][method]
                PARAMS.append([path, actor, method, expected])
            except:
                PARAMS.append([path, actor, method, None])


class TestAUTH(TestCase):

    def do_request(self, path, actor, method):
        headers = {'content-type': 'application/rdf+turtle'}
        data = None

        uri = URIs[path]

        auth = AUTH[actor]

        if path in QUERIES:
            if method == 'post':
                data = QUERIES[path]['params']
                headers = QUERIES[path]['headers']
            elif method == 'get':
                data = {'query': QUERIES[path]['params']}
                headers = QUERIES[path]['headers']


        func = METHODS[method]

        if actor is not None:
            response = func(uri, headers=headers, data=data, auth=auth)
        else:
            response = func(uri, headers=headers, data=data)

        return response

    def test_request(self):
        for path, actor, method, expected in PARAMS:
            with self.subTest(path=path, actor=actor, method=method, expected=expected):

                self.setUp()

                response = self.do_request(path, actor, method)

                if expected is not None:
                    assert_equal(response.status_code, expected)
                else:
                    assert response.status_code >= 400


    def test_one_request(self):
        response = self.do_request('repo', 'viewer', 'delete')
        assert response.status_code >= 200

    def setUp(self) :
        try:
            sparql_endpoint = triple_store.create_repository('test', auth=AUTH['admin'])
        except TripleStoreCreateRepositoryAlreadyExists:
            pass

    def tearDown(self) :
        sparql_endpoint = triple_store.drop_repository('test', auth=AUTH['admin'])
