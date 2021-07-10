# -*- coding: utf-8 -*-
"""Triple store access"""

from http import HTTPStatus

import requests


from pyrdf4j.constants import RDF4J_BASE
from pyrdf4j.errors import HarvestURINotReachable, \
    TripleStoreBulkLoadError, TripleStoreCreateRepositoryError, TripleStoreDropRepositoryError, \
    TripleStoreCannotStartTransaction, TripleStoreCannotCommitTransaction, TripleStoreTerminatingError, \
    TripleStoreCannotRollbackTransaction, TripleStoreRollbackOccurred, TripleStoreCreateRepositoryAlreadyExists
from pyrdf4j.repo_types import repo_config_factory


class Transaction():
    """
    Decorator to brace a transaction around a triple store operation.
    It is required that the repository target_uri is the first arg (after self) of the decorated function
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
            repo_id = args[1]
            # get auth information
            if 'auth' in kwargs:
                auth = kwargs['auth']
            else:
                auth = None
            # get the repo_uri from the repo id
            repo_uri = caller_self.repo_id_to_uri(repo_id)
            # open a transaction for the repo_uri and retrieve the transaction target_uri
            transaction_uri = caller_self.start_transaction(repo_uri, auth=auth)
            kwargs['repo_uri'] = transaction_uri
            # Watch for exceptions in the operation. Wrong return codes have to raise TripleStoreTerminatingError
            # to trigger a rollback
            try:
                # do the actual database operation and remember the response.
                # Notice the replacement of the repo_uri by the transaction_uri
                response = func(caller_self, *args[1:], **kwargs)
            except TripleStoreTerminatingError as e:
                # I case of a terminating error roll back the transaction
                caller_self.rollback(transaction_uri, auth=auth)
                # Reraise the original error
                raise e

            # commit the transaction
            caller_self.commit(transaction_uri, auth=auth)

            # Return the response to the actual database operation
            return response

        return wrapper


class RDF4J:
    """
    API to the RDF4J WebAPI
    """

    def __init__(self, RDF4J_base=None):

        self.repository_uris = {}
        if RDF4J_base is not None:
            self.RDF4J_base = RDF4J_base
        else:
            self.RDF4J_base = RDF4J_BASE

    @staticmethod
    def get(uri, **params):
        """Low level GET request"""
        response = requests.get(uri, params=params['data'], **params)
        return response

    @staticmethod
    def post(uri, **params):
        """Low level POST request"""
        response = requests.post(uri, **params)
        return response

    @staticmethod
    def put(uri, **params):
        """Low level PUT request"""
        response = requests.put(uri, **params)
        return response

    @staticmethod
    def delete(uri, **params):
        """Low level DELETE request"""
        response = requests.delete(uri, **params)
        return response

    @staticmethod
    def start_transaction(repo_uri, auth=None):
        # start a transaction and return the associated transaction URI
        # returns : The transaction URI

        response = requests.post(
            repo_uri + '/transactions',
            auth=auth
        )
        if response.status_code != HTTPStatus.CREATED:
            raise TripleStoreCannotStartTransaction

        return response.headers['Location']

    @staticmethod
    def commit(transcation_uri, auth=None):
        # commit a transaction and return the response status
        response = requests.put(
            transcation_uri + '?action=COMMIT',
            auth=auth
        )
        if response.status_code != HTTPStatus.OK:
            raise TripleStoreCannotCommitTransaction

        return response.status_code

    @staticmethod
    def rollback(transcation_uri, auth=None):
        # Rollback a transaction and return the response status
        response = requests.delete(
            transcation_uri + '?action=ROLLBACK',
            auth=auth
        )
        if response.status_code != HTTPStatus.OK:
            raise TripleStoreCannotRollbackTransaction

        return response.status_code

    def repo_id_to_uri(self, repo_id, repo_uri=None):
        """Translates a repository ID into a repository URI"""
        if repo_uri is not None:
            return repo_uri
        repo_uri = self.RDF4J_base + 'repositories/{}'.format(repo_id)
        return repo_uri

    def rest_create_repository(self, repo_uri, repo_config, auth=None):
        """
        Creates a repository in rdf4j
        :param repo_uri: URI of repo to be created.
        :param repo_config: Configuration of the repository as TTL resource
        :param auth: Optional user credential in form of a HTTPBasicAuth instance (testing only)
        :return: the response from the RDF4J server
        """

        headers = {'content-type': 'application/x-turtle'}
        response = self.put(
            repo_uri,
            headers=headers,
            data=repo_config,
            auth=auth,
        )
        return response

    def rest_drop_repository(self, repo_uri, auth=None):
        """
        Drops a repository
        :param repo_uri: URI of repo to be dropped.
        :param auth: (optional) authentication Instance
        :return: the response from the triple store
        """

        headers = {'content-type': 'application/x-turtle'}
        response = self.delete(
            repo_uri,
            headers=headers,
            auth=auth,
        )

        return response

    @Transaction()
    def bulk_load_from_uri(
            self,
            repo_id,
            target_uri,
            content_type,
            clear_repository=False,
            repo_label=None,
            repo_uri=None,
            auth=None,
            ):
        """
        Load the triple_data from the harvest uri
        and push it into the triplestore
        :param repo_id:
        :param target_uri:
        :param content_type:
        :return:
        """

        repo_uri = self.repo_id_to_uri(repo_id, repo_uri=repo_uri)
        # Load the triple_data from the harvest target_uri
        response = requests.get(target_uri)
        if response.status_code != HTTPStatus.OK:
            raise HarvestURINotReachable(response.content)
        triple_data = response.content

        if clear_repository:
            self.empty_repository(repo_uri)

        if repo_label is None:
            repo_label = 'None'

        response = self.create_repository(repo_id, auth=auth)
        if response.status_code == HTTPStatus.CONFLICT:
            if b'REPOSITORY EXISTS' in response.content:
                pass
        elif response.status_code != HTTPStatus.OK:
            raise TripleStoreTerminatingError

        headers = {'Content-Type': content_type}
        response = self.put(
            repo_uri+ '?action=ADD',
            data=triple_data,
            headers=headers,
            auth=auth,
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

    def create_repository(self, repo_id, repo_type='memory', repo_label=None, auth=None, overwrite=False, **kwargs):
        """
        :param repo_id: ID of the repository to create
        :param repo_type: (Optional) Configuration template type name of the repo (see repo_types.py)
        :param repo_label: (Optional) Label for the repository
        :param auth: (Optional) user credentials for authentication in form of a HTTPBasicAuth instance (testing only)
        :param overwrite: (Optional) If overwrite is enabled an existing repo will be overwritten (testing only). Use with care!
        :param kwargs: Parameters for the Configuration template
        :return:
        """
        if repo_label is None:
            repo_label = repo_id

        repo_config = repo_config_factory(
            repo_type,
            repo_id=repo_id,
            repo_label=repo_label,
            **kwargs)

        repo_uri = self.repo_id_to_uri(repo_id)

        try:
            response = self.rest_create_repository(repo_uri, repo_config, auth=auth)
            if response.status_code in [HTTPStatus.NO_CONTENT]:
                return response
            elif response.status_code == HTTPStatus.CONFLICT:
                msg = str(response.status_code) + ': ' + str(response.content)
                raise TripleStoreCreateRepositoryAlreadyExists(msg)
            else:
                msg = str(response.status_code) + ': ' + str(response.content)
                raise TripleStoreCreateRepositoryError(msg)

        except TripleStoreCreateRepositoryAlreadyExists:
            if overwrite:
                self.rest_drop_repository(repo_uri, auth=auth)
                self.rest_create_repository(repo_uri, repo_config, auth=auth)

        return response

    def drop_repository(self, repo_id, accept_not_exist=False, auth=None):
        """
        :param repo_id: ID of the repository to drop
        :return: response
        :raises: TripleStoreDropRepositoryError if operation fails
        """

        repo_uri = self.repo_id_to_uri(repo_id)

        response = self.rest_drop_repository(repo_uri, auth=auth)
        if response.status_code in [HTTPStatus.NO_CONTENT]:
            return response
        elif response.status_code in [HTTPStatus.NOT_FOUND]:
            if accept_not_exist:
                return response
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

    def get_turtle_from_query(self, repo_id, query, auth=None):
        """
        :param repository:
        :param query:
        :return:
        """
        mime_type = 'text/turtle'
        triple_data = self.get_triple_data_from_query(repo_id, query, mime_type, auth=auth)
        return triple_data

    def get_triple_data_from_query(self, repo_id, query, mime_type, auth=None):
        """
        :param repo_id:
        :param query:
        :param mime_type:
        :return:
        """
        repo_uri = self.repo_id_to_uri(repo_id)

        headers = {
            'Accept': mime_type
        }

        data = {'query': query}
        response = self.post(
            repo_uri,
            headers=headers,
            data=data,
            auth=auth)
        triple_data = response.content

        return triple_data

    def empty_repository(self, repository, auth=None):
        """
        :param repository:
        :return:
        """
        self.create_repository(repository, auth=auth)
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
triple_store = RDF4J()
