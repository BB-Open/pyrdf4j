from http import HTTPStatus

from pyrdf4j.constants import DEFAULT_QUERY_RESPONSE_MIME_TYPE, \
    DEFAULT_QUERY_MIME_TYPE, \
    DEFAULT_CHARSET
from pyrdf4j.errors import QueryFailed
from pyrdf4j.server import Transaction


class APIBase:

    def __init__(self, server, repo_id, repo_uri=None):
        self.server = server
        self.repo_id = repo_id
        self.repo_uri = self.repo_id_to_repo_uri(repo_id, repo_uri=repo_uri)
        self.uri = self.repo_id_to_uri(repo_id, repo_uri=repo_uri)

    @classmethod
    def create(cls, server, repo_id, repo_config, repo_uri=None, auth=None):
        api = cls(server, repo_id, repo_uri=repo_uri)
        response = api.create_repository(repo_config, auth=auth)
        return api, response

    def repo_id_to_repo_uri(self, repo_id, repo_uri=None):
        """Translates a repository ID into a repository endpoint URI"""
        if repo_uri is not None:
            return repo_uri
        repo_uri = self.server.RDF4J_base + 'repositories/{}'.format(repo_id)
        return repo_uri

    def repo_id_to_uri(self, repo_id, repo_uri=None):
        """Translates a repository ID into a api endpoint URI"""
        return self.repo_id_to_repo_uri(repo_id, repo_uri=repo_uri)

    def create_repository(self, repo_config, auth=None, charset=None):
        """
        Creates a repository in rdf4j
        :param repo_config: Configuration of the repository as TTL resource
        :param auth: Optional user credential in form of a HTTPBasicAuth instance (testing only)
        :return: the response from the Server server
        """

        if charset is None:
            charset = DEFAULT_CHARSET

        headers = {'content-type': 'application/x-turtle; charset=' + charset}
        response = self.server.put(
            self.repo_uri,
            headers=headers,
            data=repo_config,
            auth=auth,
        )
        return response

    def drop_repository(self, auth=None, charset=None):
        """
        Drops a repository
        :param auth: (optional) authentication Instance
        :return: the response from the triple store
        """
        if charset is None:
            charset = DEFAULT_CHARSET

        headers = {'content-type': 'application/x-turtle; charset=' + charset}
        response = self.server.delete(
            self.repo_uri,
            headers=headers,
            auth=auth,
        )

        return response

    def empty_repository(self, auth=None):
        # because we can not send a delete query,
        # we query repo config,
        # delete repo und create new one with same config
        config = self.repo_uri + '/config'
        headers = {'content-type': 'application/x-turtle; charset=' + DEFAULT_CHARSET}
        response = self.server.get(config, auth=auth, data={}, headers=headers)
        res = response.content.decode('utf8')
        self.drop_repository(auth=auth)
        # self.create_repository(res, auth=auth)

    def query_repository(self, query, query_type=None, mime_type=None, auth=None, charset=None):

        if charset is None:
            charset = DEFAULT_CHARSET

        if query_type is None:
            query_type = DEFAULT_QUERY_MIME_TYPE

        if mime_type is None:
            mime_type = DEFAULT_QUERY_RESPONSE_MIME_TYPE

        headers = {
            'Accept': mime_type,
            'content-type': query_type + '; charset=' + charset,
        }

        response = self.server.post(
            self.repo_uri,
            data=query,
            headers=headers,
            auth=auth,
        )
        if response.status_code in [HTTPStatus.OK]:
            triple_data = response.content
            triple_data = triple_data
            return triple_data
        else:
            raise QueryFailed(query)
