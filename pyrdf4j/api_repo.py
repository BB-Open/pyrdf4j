from http import HTTPStatus

from pyrdf4j.constants import DEFAULT_QUERY_RESPONSE_MIME_TYPE, DEFAULT_QUERY_MIME_TYPE
from pyrdf4j.errors import QueryFailed


class APIRepo:

    def __init__(self, server):
        self.server = server

    def create_repository(self, repo_uri, repo_config, auth=None):
        """
        Creates a repository in rdf4j
        :param repo_uri: URI of server to be created.
        :param repo_config: Configuration of the repository as TTL resource
        :param auth: Optional user credential in form of a HTTPBasicAuth instance (testing only)
        :return: the response from the Server server
        """

        headers = {'content-type': 'application/x-turtle'}
        response = self.server.put(
            repo_uri,
            headers=headers,
            data=repo_config,
            auth=auth,
        )
        return response

    def drop_repository(self, repo_uri, auth=None):
        """
        Drops a repository
        :param repo_uri: URI of server to be dropped.
        :param auth: (optional) authentication Instance
        :return: the response from the triple store
        """

        headers = {'content-type': 'application/x-turtle'}
        response = self.server.delete(
            repo_uri,
            headers=headers,
            auth=auth,
        )

        return response

    def query_repository(self, repo_uri, query, query_type=None, response_type=None, auth=None):
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


