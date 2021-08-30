from http import HTTPStatus

from pyrdf4j.api_base import APIBase
from pyrdf4j.constants import DEFAULT_QUERY_RESPONSE_MIME_TYPE, DEFAULT_QUERY_MIME_TYPE
from pyrdf4j.errors import QueryFailed, TerminatingError
from pyrdf4j.server import Transaction


class APIRepo(APIBase):

    @Transaction()
    def add_triple_data_to_repo(self, repo_id, triple_data, content_type, auth=None, repo_uri=None):
        headers = {'Content-Type': content_type}
        response = self.server.put(
            repo_uri + '?action=ADD',
            data=triple_data,
            headers=headers,
            auth=auth,
        )
        if response.status_code != HTTPStatus.OK:
            raise TerminatingError

        return response

    def replace_triple_data_in_repo(self, repo_id, triple_data, content_type, auth=None, repo_uri=None):
        self.empty_repository(repo_id, auth=auth, repo_uri=repo_uri)
        return self.add_triple_data_to_repo(repo_id, triple_data, content_type, auth=auth, repo_uri=repo_uri)



