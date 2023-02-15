from http import HTTPStatus

from pyrdf4j.api_base import APIBase
from pyrdf4j.constants import DEFAULT_CHARSET
from pyrdf4j.errors import TerminatingError


class APIGraph(APIBase):

    def repo_id_to_uri(self, repo_id, repo_uri=None):
        """Translates a repository ID into a api endpoint URI"""
        if repo_uri is not None:
            return repo_uri
        repo_uri = self.server.RDF4J_base + 'repositories/{}/statements'.format(repo_id)
        return repo_uri

    def replace_triple_data_in_repo(self, triple_data, content_type, auth=None, charset=None, base_uri=None):

        if charset is None:
            charset = DEFAULT_CHARSET

        headers = {'Content-Type': content_type + '; charset=' + charset}
        self.empty_repository(auth=auth)

        response = self.server.put(
            self.uri,
            data=triple_data,
            headers=headers,
            auth=auth,
        )
        if response.status_code != HTTPStatus.NO_CONTENT:
            raise TerminatingError(response.content)

        return response

    def add_triple_data_to_repo(self, triple_data, content_type, auth=None, charset=None, base_uri=None):
        if charset is None:
            charset = DEFAULT_CHARSET
        headers = {'Content-Type': content_type + '; charset=' + charset}
        response = self.server.post(
            self.uri,
            data=triple_data,
            headers=headers,
            auth=auth,
        )
        if response.status_code != HTTPStatus.NO_CONTENT:
            raise TerminatingError(response.content)

        return response

    def empty_repository(self, auth=None):
        mime_type = 'application/rdf+xml'
        query = '''DELETE {?s ?p ?o . } Where {?s ?p ?o}'''
        #
        headers = {
            'Accept': mime_type
        }
        data = {'query': query}
        response = self.server.delete(self.uri, headers=headers, data=data, auth=auth)
        triple_data = response.content
        return triple_data
