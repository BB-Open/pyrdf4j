# -*- coding: utf-8 -*-
"""RDF4J Rest-API access"""
import sys
import traceback
from http import HTTPStatus

import requests

from pyrdf4j.constants import RDF4J_BASE, DEFAULT_CONTENT_TYPE, DEFAULT_QUERY_MIME_TYPE, \
    DEFAULT_QUERY_RESPONSE_MIME_TYPE
from pyrdf4j.errors import CannotStartTransaction, CannotCommitTransaction, TerminatingError, \
    CannotRollbackTransaction, QueryFailed, DataBaseNotReachable


class Transaction():
    """
    Decorator to brace a transaction around a triple store operation.
    It is required that the repository target_uri is the
    first arg (after self) of the decorated function
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
            # get auth information
            if 'auth' in kwargs:
                auth = kwargs['auth']
            else:
                auth = None
            # get the repo_uri from the server id
            uri = caller_self.uri
            repo_uri = caller_self.repo_uri
            # open a transaction for the repo_uri and retrieve the transaction target_uri
            transaction_uri = caller_self.server.start_transaction(uri, auth=auth)
            caller_self.uri = transaction_uri
            caller_self.repo_uri = transaction_uri
            # Watch for exceptions in the operation.
            # Wrong return codes have to raise TerminatingError
            # to trigger a rollback
            try:
                # do the actual database operation and remember the response.
                # Notice the replacement of the repo_uri by the transaction_uri
                response = func(caller_self, *args[1:], **kwargs)
            except TerminatingError as e:
                # I case of a terminating error roll back the transaction
                try:
                    caller_self.server.rollback(transaction_uri, auth=auth)
                except CannotRollbackTransaction:
                    pass
                caller_self.uri = uri
                caller_self.repo_uri = repo_uri
                # Reraise the original error
                raise e

            # commit the transaction
            caller_self.server.commit(transaction_uri, auth=auth)
            caller_self.uri = uri
            caller_self.repo_uri = repo_uri

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
        try:
            response = requests.get(uri, params=params['data'], **params)
            return response
        except requests.exceptions.ConnectionError as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            # todo: Logger
            print("GET termiated due to error %s %s" % (exc_type, exc_value))
            for line in traceback.format_tb(exc_traceback):
                print("Traceback:%s" % line[:-1])
            raise DataBaseNotReachable(
                'Database not reachable. Tried GET on {uri} with params {params}'.format(
                    uri=uri,
                    params=params
                )
            )

    @staticmethod
    def post(uri, **params):
        """Low level POST request"""
        try:
            response = requests.post(uri, **params)
            return response
        except requests.exceptions.ConnectionError as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            # todo: Logger
            print("POST termiated due to error %s %s" % (exc_type, exc_value))
            for line in traceback.format_tb(exc_traceback):
                print("Traceback:%s" % line[:-1])
            raise DataBaseNotReachable(
                'Database not reachable. Tried POST on {uri} with params {params}'.format(
                    uri=uri,
                    params=params
                )
            )

    @staticmethod
    def put(uri, **params):
        """Low level PUT request"""
        try:
            response = requests.put(uri, **params)
            return response
        except requests.exceptions.ConnectionError as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            # todo: Logger
            print("PUT termiated due to error %s %s" % (exc_type, exc_value))
            for line in traceback.format_tb(exc_traceback):
                print("Traceback:%s" % line[:-1])
            raise DataBaseNotReachable(
                'Database not reachable. Tried PUT on {uri} with params {params}'.format(
                    uri=uri,
                    params=params
                )
            )

    @staticmethod
    def delete(uri, **params):
        """Low level DELETE request"""
        try:
            response = requests.delete(uri, **params)
            return response
        except requests.exceptions.ConnectionError as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            # todo: Logger
            print("DELETE termiated due to error %s %s" % (exc_type, exc_value))
            for line in traceback.format_tb(exc_traceback):
                print("Traceback:%s" % line[:-1])
            raise DataBaseNotReachable(
                'Database not reachable. Tried DELETE on {uri} with params {params}'.format(
                    uri=uri,
                    params=params
                )
            )

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
