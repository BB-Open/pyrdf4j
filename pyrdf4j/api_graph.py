from http import HTTPStatus

from pyrdf4j.api_base import APIBase
from pyrdf4j.constants import DEFAULT_QUERY_RESPONSE_MIME_TYPE, DEFAULT_QUERY_MIME_TYPE
from pyrdf4j.errors import QueryFailed


class APIGraph(APIBase):

    def repo_id_to_uri(self, repo_id, repo_uri=None):
        """Translates a repository ID into a repository URI"""
        if repo_uri is not None:
            return repo_uri
        repo_uri = self.server.RDF4J_base + 'repositories/{}/statements'.format(repo_id)
        return repo_uri

    def query_repository(self, repo_id, query, query_type=None, response_type=None, auth=None):
        if query_type is None:
            query_type = 'text/turtle'
        super(APIGraph, self).query_repository(
            repo_id,
            query,
            query_type=query_type,
            response_type=response_type,
            auth=auth
        )
