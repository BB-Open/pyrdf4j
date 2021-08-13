from http import HTTPStatus

from pyrdf4j.constants import DEFAULT_QUERY_RESPONSE_MIME_TYPE, DEFAULT_QUERY_MIME_TYPE
from pyrdf4j.errors import QueryFailed
from pyrdf4j.server import Transaction


class APIBase:

    def __init__(self, server):
        self.server = server

    def repo_id_to_uri(self, repo_id, repo_uri=None):
        """Translates a repository ID into a repository endpoint URI"""
        if repo_uri is not None:
            return repo_uri
        repo_uri = self.server.RDF4J_base + 'repositories/{}'.format(repo_id)
        return repo_uri

    def create_repository(self, repo_id, repo_config, auth=None):
        """
        Creates a repository in rdf4j
        :param repo_id: ID of repository to be created
        :param repo_config: Configuration of the repository as TTL resource
        :param auth: Optional user credential in form of a HTTPBasicAuth instance (testing only)
        :return: the response from the Server server
        """

        repo_uri = self.repo_id_to_uri(repo_id)
        headers = {'content-type': 'application/x-turtle'}
        response = self.server.put(
            repo_uri,
            headers=headers,
            data=repo_config,
            auth=auth,
        )
        return response

    def drop_repository(self, repo_id, auth=None):
        """
        Drops a repository
        :param repo_id: ID of repository to be dropped
        :param auth: (optional) authentication Instance
        :return: the response from the triple store
        """

        repo_uri = self.repo_id_to_uri(repo_id)
        headers = {'content-type': 'application/x-turtle'}
        response = self.server.delete(
            repo_uri,
            headers=headers,
            auth=auth,
        )

        return response

    @Transaction()
    def query_repository(self, repo_id, query, query_type=None, response_type=None, auth=None):

        repo_uri = self.repo_id_to_uri(repo_id)
        if query_type is None:
            query_type = DEFAULT_QUERY_MIME_TYPE

        if response_type is None:
            response_type = DEFAULT_QUERY_RESPONSE_MIME_TYPE

        headers = {
            'Accept': response_type,
            'content-type': query_type,
        }

        response = self.server.post(
            repo_uri,
            data=query,
            headers=headers,
            auth=auth,
        )

        if response.status_code in [HTTPStatus.OK]:
            triple_data = response.content
            return triple_data
        else:
            raise QueryFailed(query)


