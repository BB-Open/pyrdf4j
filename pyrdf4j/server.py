# -*- coding: utf-8 -*-
"""RDF4J Rest-API access"""

from http import HTTPStatus

import requests

from pyrdf4j.constants import RDF4J_BASE, DEFAULT_CONTENT_TYPE, DEFAULT_QUERY_MIME_TYPE, \
    DEFAULT_QUERY_RESPONSE_MIME_TYPE
from pyrdf4j.errors import CannotStartTransaction, CannotCommitTransaction, TerminatingError, \
    CannotRollbackTransaction, QueryFailed


class Transaction():
    """
    Decorator to brace a transaction around a triple store operation.
    It is required that the repository target_uri is the first arg (after self) of the decorated function
    Returns: The response to the actual triple store operation
    Raises: Raises TerminatingError if a rollback was necessary

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
            # get the repo_uri from the server id
            repo_uri = caller_self.repo_id_to_uri(repo_id)
            # open a transaction for the repo_uri and retrieve the transaction target_uri
            transaction_uri = caller_self.server.start_transaction(repo_uri, auth=auth)
            kwargs['repo_uri'] = transaction_uri
            # Watch for exceptions in the operation. Wrong return codes have to raise TerminatingError
            # to trigger a rollback
            try:
                # do the actual database operation and remember the response.
                # Notice the replacement of the repo_uri by the transaction_uri
                response = func(caller_self, *args[1:], **kwargs)
            except TerminatingError as e:
                # I case of a terminating error roll back the transaction
                caller_self.rollback(transaction_uri, auth=auth)
                # Reraise the original error
                raise e

            # commit the transaction
            caller_self.server.commit(transaction_uri, auth=auth)

            # Return the response to the actual database operation
            return response

        return wrapper


class Server:
    """
    Represent a RDF4J server instance
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
            raise CannotStartTransaction

        return response.headers['Location']

    @staticmethod
    def commit(transcation_uri, auth=None):
        # commit a transaction and return the response status
        response = requests.put(
            transcation_uri + '?action=COMMIT',
            auth=auth
        )
        if response.status_code != HTTPStatus.OK:
            raise CannotCommitTransaction

        return response.status_code

    @staticmethod
    def rollback(transcation_uri, auth=None):
        # Rollback a transaction and return the response status
        response = requests.delete(
            transcation_uri + '?action=ROLLBACK',
            auth=auth
        )
        if response.status_code != HTTPStatus.OK:
            raise CannotRollbackTransaction

        return response.status_code


