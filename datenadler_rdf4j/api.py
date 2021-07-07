# -*- coding: utf-8 -*-
"""Triple store access"""

from http import HTTPStatus

import requests
from requests.auth import HTTPBasicAuth
from SPARQLWrapper import SPARQLWrapper2

from datenadler_rdf4j.constants import RDF4J_BASE, ADMIN_PASS, ADMIN_USER
from datenadler_rdf4j.errors import HarvestURINotReachable, \
    TripleStoreBulkLoadError, TripleStoreCreateRepositoryError, TripleStoreDropRepositoryError, \
    TripleStoreCannotStartTransaction, TripleStoreCannotCommitTransaction, TripleStoreTerminatingError, \
    TripleStoreCannotRollbackTransaction, TripleStoreRollbackOccurred, TripleStoreCreateRepositoryAlreadyExists


class SPARQL():
    """
    API to the SPARQL Endpoint of a repository
    """

    def __init__(self, uri):
        self.sparql = SPARQLWrapper2(uri)

    def exists(self, _URI):
        """
        :param _URI:
        :return:
        """
        self.sparql.setQuery("""
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            SELECT ?s
            WHERE { ?s ?p ?o }
                    """)
        results = self.sparql.query()
        if results:
            return True

    def insert(self, triple):
        """
        :param triple:
        :return:
        """
        queryString = """INSERT DATA
           {{ GRAPH <http://example.com/> {{ {s} {p} {o} }} }}"""

        self.sparql.setQuery(
            queryString.format(s=triple.s, o=triple.o, p=triple.p))
        self.sparql.method = 'POST'
        self.sparql.query()

    def query(self, queryString):
        """
        :param queryString:
        :return:
        """
        self.sparql.setQuery(queryString)
        results = self.sparql.query()
        return results



class Transaction():
    """
    Decorator to brace a transaction around a triple store operation.
    It is required that the repository uri is the first arg (after self) of the decorated function
    Returns: The response to the actual triple store operation
    Raises: Raises TripleStoreTerminatingError if a rollback was necessary

    """

    def __call__(self, func):
        """
        Do the transaction bracing
        """
        def wrapper(*args, **kwargs):
            # get the self of the caller
            caller_self = args[0]
            # get the repo_uri as the first argument (after self)
            repo_uri = args[1]
            # open a transaction for the repo_uri and retrieve the transaction uri
            transaction_uri = self.start_transaction(repo_uri)
            # Watch for exceptions in the operation. Wrong return codes have to raise TripleStoreTerminatingError
            # to trigger a rollback
            try:
                # do the actual database operation and remember the response.
                # Notice the replacement of the repo_uri by the transaction_uri
                response = func(caller_self, transaction_uri, *args[2:], **kwargs)
            except TripleStoreTerminatingError as e:
                # I case of a terminating error roll back the transaction
                self.rollback(transaction_uri)
                # Reraise the original error
                raise e

            # commit the transaction
            self.commit(transaction_uri)

            # Return the response to the actual database operation
            return response

        return wrapper

    @staticmethod
    def start_transaction(repo_uri):
        # start a transaction and return the associated transaction URI
        # returns : The transaction URI

        response = requests.post(
            repo_uri + '/transactions'
        )
        if response.status_code != HTTPStatus.CREATED:
            raise TripleStoreCannotStartTransaction

        return response.headers['Location']

    @staticmethod
    def commit(transcation_uri):
        # commit a transaction and return the response status
        response = requests.put(
            transcation_uri + '?action=COMMIT'
        )
        if response.status_code != HTTPStatus.OK:
            raise TripleStoreCannotCommitTransaction

        return response.status_code

    @staticmethod
    def rollback(transcation_uri):
        # Rollback a transaction and return the response status
        response = requests.delete(
            transcation_uri + '?action=ROLLBACK'
        )
        if response.status_code != HTTPStatus.OK:
            raise TripleStoreCannotRollbackTransaction

        return response.status_code



class Triplestore():
    """
    API to the triplestore
    """

    def __init__(self, RDF4J_base=None):

        self.repository_uris = {}
        if RDF4J_base is not None:
            self.RDF4J_base = RDF4J_base
        else:
            self.RDF4J_base = RDF4J_BASE

    def get(self, uri, **params):
        """Low level GET request"""
        response = requests.get(uri, **params)
        return response

    def post(self, uri, **params):
        """Low level POST request"""
        response = requests.post(uri, **params)
        return response

    def put(self, uri, **params):
        """Low level PUT request"""
        response = requests.put(uri, **params)
        return response

    def delete(self, uri, **params):
        """Low level DELETE request"""
        response = requests.delete(uri, **params)
        return response

    def repo_id_to_uri(self, repo_id):
        repo_uri = self.RDF4J_base + 'repositories/{}'.format(repo_id)
        return repo_uri


    def rest_create_repository(self, repo_id, repo_label, auth=None):
        """
        Creates a repository in the triplestore and registers
        a repository sparqlwrapper for it
        :param repo_id: Id of the repository to be created
        :param repo_label: Description of the repository to be created
        :param auth: Optional user credential in form of a HTTPBasicAuth instance (testing only)
        :return: the response from the RDF4J server
        """
        if auth is None:
            auth = HTTPBasicAuth(ADMIN_USER, ADMIN_PASS)

        repo_uri = self.repo_id_to_uri(repo_id)

        params = """
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>.
@prefix rep: <http://www.openrdf.org/config/repository#>.
@prefix sr: <http://www.openrdf.org/config/repository/sail#>.
@prefix sail: <http://www.openrdf.org/config/sail#>.
@prefix ns: <http://www.openrdf.org/config/sail/native#>.

[] a rep:Repository ;
   rep:repositoryID "{repo_id}" ;
   rdfs:label "{repo_label}" ;
   rep:repositoryImpl [
      rep:repositoryType "openrdf:SailRepository" ;
      sr:sailImpl [
        sail:sailType "openrdf:NativeStore" ;
         sail:iterationCacheSyncThreshold "10000";
         ns:tripleIndexes "spoc,posc"        
      ]
   ].
""".format(repo_id=repo_id, repo_label=repo_label)
        headers = {'content-type': 'application/x-turtle'}
        response = self.put(
            repo_uri,
            data=params,
            headers=headers,
            auth=auth,
        )

        return response

    def rest_drop_repository(self, repo_id, auth=None):
        """
        Drops a repository in the triple store and deletes the repository sparqlwrapper of it
        :param repository_id: Id of the repository to be dropped
        :return: the response from the triple store
        """

        if auth is None:
            auth = HTTPBasicAuth(ADMIN_USER, ADMIN_PASS)

        repo_uri = self.repo_id_to_uri(repo_id)

        headers = {'content-type': 'application/x-turtle'}
        response = self.delete(
            repo_uri,
            headers=headers,
            auth=auth,
        )

        return response



    @Transaction()
    def rest_bulk_load_from_uri(self, repo_uri, uri, content_type, clear_repository=False, repo_label=None):
        """
        Load the triple_data from the harvest uri
        and push it into the triplestore
        :param repository_id:
        :param uri:
        :param content_type:
        :return:
        """
        # Load the triple_data from the harvest uri
        response = requests.get(uri)
        if response.status_code != HTTPStatus.OK:
            raise HarvestURINotReachable(response.content)
        triple_data = response.content

        if clear_repository:
            self.empty_repository(repo_uri)

        if repo_label is None:
            repo_label = 'None'
        self.rest_create_repository(repo_uri, repo_label)

        headers = {'Content-Type': content_type}
        response = self.put(
            repo_uri+ '?action=ADD',
            data=triple_data,
            headers=headers,
        )
        if response.status_code != HTTPStatus.OK:
            raise TripleStoreTerminatingError

        return response

    def graph_from_uri(self, repository_id, uri, content_type, clear_repository=False):
        """
        :param repository_id:
        :param uri:
        :param content_type:
        :param clear_repository:
        :return:
        """
        self.create_repository(repository_id)
        response = self.rest_bulk_load_from_uri(
            repository_id, uri, content_type, clear_repository=clear_repository)
        if response.status_code == 200:
            return self.sparql_for_repository(repository_id), response
        else:
            raise TripleStoreBulkLoadError(response.content)

    def create_repository(self, repository_id, repository_label=None, auth=None):
        """
        :param repository_id: ID of the repository to create
        :param repository_label: (Optional) Label for the repository
        :param auth: Optional user credential in form of a HTTPBasicAuth instance (testing only)
        :return:
        """
        if repository_label is None:
            repository_label = repository_id
        response = self.rest_create_repository(repository_id, repository_label, auth=auth)
        if response.status_code in [HTTPStatus.NO_CONTENT]:
            return response
        elif response.status_code == HTTPStatus.CONFLICT:
            msg = str(response.status_code) + ': ' + str(response.content)
            raise TripleStoreCreateRepositoryAlreadyExists(msg)
        else:
            msg = str(response.status_code) + ': ' + str(response.content)
            raise TripleStoreCreateRepositoryError(msg)

    def drop_repository(self, repository_id):
        """
        :param repository_id:
        :return:
        """
        response = self.rest_drop_repository(repository_id)
        if response.status_code in [204]:
            return response
        else:
            msg = str(response.status_code) + ': ' + str(response.content)
            raise TripleStoreDropRepositoryError(msg)




    def move_data_between_repositorys(self, target_repository, source_repository):
        """
        :param target_repository:
        :param source_repository:
        :return:
        """
        self.create_repository(source_repository)
        self.create_repository(target_repository)
        source = self.cache_repository_uri(source_repository)
        target = self.cache_repository_uri(target_repository)
        mime_type = 'application/rdf+xml'
        headers = {
            'Accept': mime_type,
        }

        data = {
            'query': 'CONSTRUCT  WHERE { ?s ?p ?o }'
        }

        response = requests.post(source, headers=headers, data=data)
        triple_data = response.content

        headers = {'Content-Type': mime_type}
        response = requests.post(
            target,
            data=triple_data,
            headers=headers,
        )

        return response

    def get_turtle_from_query(self, repository, query):
        """
        :param repository:
        :param query:
        :return:
        """
        mime_type = 'text/turtle'
        triple_data = self.get_triple_data_from_query(repository, query, mime_type)
        return triple_data

    def get_triple_data_from_query(self, repository, query, mime_type):
        """
        :param repository:
        :param query:
        :param mime_type:
        :return:
        """
        self.create_repository(repository)
        source = self.cache_repository_uri(repository)

        headers = {
            'Accept': mime_type
        }

        data = {'query': query}
        response = requests.post(source, headers=headers, data=data)
        triple_data = response.content

        return triple_data

    def empty_repository(self, repository):
        """
        :param repository:
        :return:
        """
        self.create_repository(repository)
        source = self.cache_repository_uri(repository)
        mime_type = 'application/rdf+xml'
        query = '''DELETE {?s ?p ?o . } Where {?s ?p ?o}'''

        headers = {
            'Accept': mime_type
        }

        data = {'query': query}
        response = requests.delete(source, headers=headers, data=data)
        triple_data = response.content

        return triple_data

# ToDo make to utility
triple_store = Triplestore()
