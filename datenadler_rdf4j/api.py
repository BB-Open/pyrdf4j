# -*- coding: utf-8 -*-
"""Tripel store access"""

import requests
from requests.auth import HTTPBasicAuth
from SPARQLWrapper import SPARQLWrapper2

from datenadler_rdf4j.constants import RDF4J_BASE, ADMIN_PASS, ADMIN_USER
from datenadler_rdf4j.errors import HarvestURINotReachable, \
    TripelStoreBulkLoadError, TripelStoreCreateRepositoryError, TripelStoreDropRepositoryError


class SPARQL(object):
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

    def insert(self, tripel):
        """
        :param tripel:
        :return:
        """
        queryString = """INSERT DATA
           {{ GRAPH <http://example.com/> {{ {s} {p} {o} }} }}"""

        self.sparql.setQuery(
            queryString.format(s=tripel.s, o=tripel.o, p=tripel.p))
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


class Tripelstore(object):
    """
    API to the tripelstore
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


    def sparql_for_repository(self, repository):
        """
        :param repository:
        :return:
        """
        if repository not in self.repository_uris:
            self.cache_repository_uri(repository)
        return SPARQL(self.repository_uris[repository])

    def rest_create_repository(self, repository_id, repository_label):
        """
        Creates a repository in the tripelstore and registers
        a repository sparqlwrapper for it
        :param repository_id: Id of the repository to be created
        :param repository_label: Description of the repository to be created
        :return: the response from the RDF4J server
        """
        params = """
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>.
@prefix rep: <http://www.openrdf.org/config/repository#>.
@prefix sr: <http://www.openrdf.org/config/repository/sail#>.
@prefix sail: <http://www.openrdf.org/config/sail#>.
@prefix ns: <http://www.openrdf.org/config/sail/native#>.

[] a rep:Repository ;
   rep:repositoryID "{repository_id}" ;
   rdfs:label "{repository_label}" ;
   rep:repositoryImpl [
      rep:repositoryType "openrdf:SailRepository" ;
      sr:sailImpl [
        sail:sailType "openrdf:NativeStore" ;
         sail:iterationCacheSyncThreshold "10000";
         ns:tripleIndexes "spoc,posc"        
      ]
   ].
""".format(repository_id=repository_id, repository_label=repository_label)
        headers = {'content-type': 'application/x-turtle'}
        response = self.put(
            self.RDF4J_base + 'repositories/{}'.format(repository_id),
            data=params,
            headers=headers,
            auth=HTTPBasicAuth(ADMIN_USER, ADMIN_PASS),
        )
        self.cache_repository_uri(repository_id)

        return response

    def rest_drop_repository(self, repository_id):
        """
        Drops a repository in the tripel store and deletes the repository sparqlwrapper of it
        :param repository_id: Id of the repository to be dropped
        :return: the response from the tripel store
        """
        headers = {'content-type': 'application/x-turtle'}
        repo_uri = self.repository_uris[repository_id]
        response = self.delete(
            repo_uri,
            headers=headers,
            auth=HTTPBasicAuth(ADMIN_USER, ADMIN_PASS),
        )

        self.uncache_repository_uri(repository_id)
        return response

    def cache_repository_uri(self, repository_id):
        """
        Cache a repository_id
        :param repository_id: The repository ID
        :return:
        """
        blaze_uri = self.RDF4J_base + \
                    '/repositories/{}'
        blaze_uri_with_repository = blaze_uri.format(repository_id)
        self.repository_uris[repository_id] = blaze_uri_with_repository
        return blaze_uri_with_repository

    def uncache_repository_uri(self, repository):
        """
        UnCache a repository_id
        :param repository_id: The repository ID
        :return:
        """
        if repository in self.repository_uris:
            del self.repository_uris[repository]

    def rest_bulk_load_from_uri(self, repository, uri, content_type, clear_repository=False):
        """
        Load the tripel_data from the harvest uri
        and push it into the tripelstore
        :param repository:
        :param uri:
        :param content_type:
        :return:
        """
        # Load the tripel_data from the harvest uri
        response = requests.get(uri)
        if response.status_code != 200:
            raise HarvestURINotReachable(response.content)
        tripel_data = response.content

        if clear_repository:
            self.empty_repository(repository)

        # push it into the tripelstore
        blaze_uri_with_repository = self.cache_repository_uri(repository)
        headers = {'Content-Type': content_type}
        response = requests.post(
            blaze_uri_with_repository,
            data=tripel_data,
            headers=headers,
        )
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
            raise TripelStoreBulkLoadError(response.content)

    def create_repository(self, repository_id, repository_label=None):
        """
        :param repository:
        :return:
        """
        if repository_label is None:
            repository_label = repository_id
        response = self.rest_create_repository(repository_id, repository_label)
        if response.status_code in [200, 201, 204, 409]:
            return self.sparql_for_repository(repository_id)
        else:
            msg = str(response.status_code) + ': ' + str(response.content)
            raise TripelStoreCreateRepositoryError(msg)

    def drop_repository(self, repository_id):
        """
        :param repository_id:
        :return:
        """
        response = self.rest_drop_repository(repository_id)
        if response.status_code in [204]:
            return True
        else:
            msg = str(response.status_code) + ': ' + str(response.content)
            raise TripelStoreDropRepositoryError(msg)



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
        tripel_data = response.content

        headers = {'Content-Type': mime_type}
        response = requests.post(
            target,
            data=tripel_data,
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
        tripel_data = self.get_triple_data_from_query(repository, query, mime_type)
        return tripel_data

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
        tripel_data = response.content

        return tripel_data

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
        tripel_data = response.content

        return tripel_data

# ToDo make to utility
tripel_store = Tripelstore()
