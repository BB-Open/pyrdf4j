
from nose.tools import assert_equal

from pyrdf4j import __version__

from unittest import TestCase

from pyrdf4j.rdf4j import RDF4J
from pyrdf4j.constants import RDF4J_BASE
from pyrdf4j.errors import CreateRepositoryAlreadyExists
from tests.constants import ACTORS, AUTH, RDF4J_BASE_TEST


def test_version():
    assert __version__ == '0.1.0'

rdf4j = RDF4J(RDF4J_BASE_TEST)

PUT = rdf4j.server.put
GET = rdf4j.server.get
DELETE = rdf4j.server.delete
POST = rdf4j.server.post


repos_dir = 'repos_dir'
system_dir = 'system_dir'
repo = 'server'
sparql = 'sparql'

URIs = {
    repos_dir : RDF4J_BASE + 'repositories',
    system_dir : RDF4J_BASE + 'repositories/SYSTEM',
    repo : RDF4J_BASE + 'repositories/test',
    sparql : RDF4J_BASE + 'repositories/test/statements',
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
        'viewer': {'get': 403,
                   'post': 403,
                   }
    },
    repo: {
        'admin' : {'get': 200,
                   'post': 200,
                   'put': 409,
                   'delete': 204,
                   },
        'editor' : {'get': 200,
                   'post': 200,
                   'put': 403,
                   'delete': 403,
                   },
        'viewer': {'get': 200,
                   'post': 200,
                   'put': 403,
                   'delete': 403,
                   }
    },
    sparql:{
        'admin' : {'get': 200,
                   'post': 415,
                   'put': 415,
                   'delete': 204,
                   },
        'editor' : {'get': 200,
                   'post': 415,
                   'put': 403,
                   'delete': 403,
                   },
        'viewer' : {'get': 200,
                   'post': 415,
                   'put': 403,
                   'delete': 403,
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
        response = self.do_request('server', 'viewer', 'delete')
        assert response.status_code >= 200

    def setUp(self) :
        try:
            sparql_endpoint = rdf4j.create_repository('test', auth=AUTH['admin'])
        except CreateRepositoryAlreadyExists:
            pass

    def tearDown(self) :
        sparql_endpoint = rdf4j.drop_repository('test', accept_not_exist=True, auth=AUTH['admin'])
