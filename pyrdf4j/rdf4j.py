import sys
import traceback
from http import HTTPStatus

import requests

from pyrdf4j.api_repo import APIRepo
from pyrdf4j.constants import DEFAULT_QUERY_RESPONSE_MIME_TYPE
from pyrdf4j.errors import URINotReachable, TerminatingError, BulkLoadError, \
    CreateRepositoryAlreadyExists, CreateRepositoryError, DropRepositoryError
from pyrdf4j.server import Server, Transaction
from pyrdf4j.repo_types import repo_config_factory


class RDF4J:
    """
    High level API to the RDF4J
    """

    def __init__(self, rdf4j_base=None, api=APIRepo):

        self.server = Server(rdf4j_base)
        self.api = api(self.server)

    def bulk_load_from_uri(
            self,
            repo_id,
            target_uri,
            content_type,
            clear_repository=False,
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

        # Load the triple_data from the harvest target_uri
        try:
            response = requests.get(target_uri)
        except requests.exceptions.ConnectionError as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            # todo: Logger
            print("GET termiated due to error %s %s" % (exc_type, exc_value))
            for line in traceback.format_tb(exc_traceback):
                print("Traceback:%s" % line[:-1])
            raise URINotReachable(
                'Database not reachable. Tried GET on {uri}'.format(uri=target_uri))
        if response.status_code != HTTPStatus.OK:
            raise URINotReachable(response.content)
        triple_data = response.content

        if clear_repository:
            self.api.replace_triple_data_in_repo(repo_id, triple_data, content_type, auth=auth, repo_uri=repo_uri)

        #        response = self.create_repository(repo_id, auth=auth)
        #        if response.status_code == HTTPStatus.CONFLICT:
        #            if b'REPOSITORY EXISTS' in response.content:
        #                pass
        #        elif response.status_code != HTTPStatus.OK:
        #            raise TerminatingError

        return self.api.add_triple_data_to_repo(repo_id, triple_data, content_type, auth=auth, repo_uri=repo_uri)

    def graph_from_uri(self, repository_id, target_uri, content_type, repo_type='memory', repo_label=None, auth=None,
                       overwrite=False, accept_existing=True, clear_repository=False, **kwargs):
        """
        :param repository_id:
        :param uri:
        :param content_type:
        :param clear_repository:
        :return:
        """
        self.create_repository(repository_id, accept_existing=accept_existing, repo_type=repo_type,
                               repo_label=repo_label, auth=auth, overwrite=overwrite, **kwargs)
        response = self.bulk_load_from_uri(
            repository_id, target_uri, content_type, clear_repository=clear_repository, auth=auth)
        return response

    def create_repository(self, repo_id, repo_type='memory', repo_label=None, auth=None, overwrite=False,
                          accept_existing=False, **kwargs):
        """
        :param repo_id: ID of the repository to create
        :param repo_type: (Optional) Configuration template type name of the server (see repo_types.py)
        :param repo_label: (Optional) Label for the repository
        :param auth: (Optional) user credentials for authentication in form of a HTTPBasicAuth instance (testing only)
        :param overwrite: (Optional) If overwrite is enabled an existing server will be overwritten (testing only). Use with care!
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

        response = self.api.create_repository(repo_id, repo_config, auth=auth)

        try:
            if response.status_code in [HTTPStatus.NO_CONTENT]:
                return response
            elif response.status_code == HTTPStatus.CONFLICT:
                msg = str(response.status_code) + ': ' + str(response.content)
                raise CreateRepositoryAlreadyExists(msg)
            else:
                msg = str(response.status_code) + ': ' + str(response.content)
                raise CreateRepositoryError(msg)

        except CreateRepositoryAlreadyExists:
            if overwrite:
                self.api.drop_repository(repo_id, auth=auth)
                self.api.create_repository(repo_id, repo_config, auth=auth)
            elif accept_existing:
                pass
            else:
                raise CreateRepositoryAlreadyExists(msg)

        return response

    def drop_repository(self, repo_id, accept_not_exist=False, auth=None):
        """
        :param repo_id: ID of the repository to drop
        :return: response
        :raises: DropRepositoryError if operation fails
        """

        response = self.api.drop_repository(repo_id, auth=auth)
        if response.status_code in [HTTPStatus.NO_CONTENT]:
            return response
        elif response.status_code in [HTTPStatus.NOT_FOUND]:
            if accept_not_exist:
                return response
        msg = str(response.status_code) + ': ' + str(response.content)
        raise DropRepositoryError(msg)

    def move_data_between_repositorys(self, target_repository, source_repository, auth=None):
        """
        :param target_repository:
        :param source_repository:
        :param auth:
        :return:
        """
        self.create_repository(source_repository, accept_existing=True, auth=auth)
        self.create_repository(target_repository, accept_existing=True, auth=auth)

        triple_data = self.api.query_repository(source_repository, "CONSTRUCT {?s ?o ?p} WHERE {?s ?o ?p}", auth=auth)

        response = self.api.add_triple_data_to_repo(target_repository, triple_data, DEFAULT_QUERY_RESPONSE_MIME_TYPE,
                                                    auth=auth)

        return response

    def get_turtle_from_query(self, repo_id, query, auth=None):
        """
        :param repository:
        :param query:
        :return:
        """
        mime_type = 'text/turtle'
        triple_data = self.get_triple_data_from_query(repo_id, query, mime_type=mime_type, auth=auth)
        return triple_data

    def get_triple_data_from_query(self, repo_id, query, mime_type=None, auth=None, repo_uri=None):
        """
        :param repo_id:
        :param query:
        :param mime_type:
        :return:
        """

        return self.api.query_repository(repo_id, query, mime_type=mime_type, auth=auth, repo_uri=repo_uri)

    def empty_repository(self, repository, auth=None):
        """
        :param repository:
        :return:
        """
        # self.create_repository(repository, auth=auth)
        self.api.empty_repository(repository, auth=auth)
