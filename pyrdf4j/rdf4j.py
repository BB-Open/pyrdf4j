from http import HTTPStatus

import requests

from pyrdf4j.api_repo import APIRepo
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

        # Load the triple_data from the harvest target_uri
        response = requests.get(target_uri)
        if response.status_code != HTTPStatus.OK:
            raise URINotReachable(response.content)
        triple_data = response.content

        if clear_repository:
            self.empty_repository(repo_uri)

        if repo_label is None:
            repo_label = 'None'

#        response = self.create_repository(repo_id, auth=auth)
#        if response.status_code == HTTPStatus.CONFLICT:
#            if b'REPOSITORY EXISTS' in response.content:
#                pass
#        elif response.status_code != HTTPStatus.OK:
#            raise TerminatingError

        repo_uri = self.api.repo_id_to_uri(repo_id)

        headers = {'Content-Type': content_type}
        response = self.server.put(
            repo_uri+ '?action=ADD',
            data=triple_data,
            headers=headers,
            auth=auth,
        )
        if response.status_code != HTTPStatus.OK:
            raise TerminatingError

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
        response = self.server.rest_bulk_load_from_uri(
            repository_id, uri, content_type, clear_repository=clear_repository)
        if response.status_code == 200:
            return self.server.sparql_for_repository(repository_id), response
        else:
            raise BulkLoadError(response.content)

    def create_repository(self, repo_id, repo_type='memory', repo_label=None, auth=None, overwrite=False, **kwargs):
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

        try:
            response = self.api.create_repository(repo_id, repo_config, auth=auth)
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

    def get_triple_data_from_query(self, repo_id, query, response_type=None, auth=None):
        """
        :param repo_id:
        :param query:
        :param mime_type:
        :return:
        """

        return self.api.query_repository(repo_id, query, response_type=response_type, auth=auth)

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

