from http import HTTPStatus

from pyrdf4j.api_base import APIBase
from pyrdf4j.constants import DEFAULT_CHARSET
from pyrdf4j.errors import TerminatingError
from pyrdf4j.server import Transaction


class APIRepo(APIBase):

    @Transaction()
    def add_triple_data_to_repo(self, triple_data, content_type, auth=None, charset=None):
        if charset is None:
            charset = DEFAULT_CHARSET

        headers = {'Content-Type': content_type + '; charset=' + charset}
        response = self.server.put(
            self.uri + '?action=ADD',
            data=triple_data,
            headers=headers,
            auth=auth,
        )
        if response.status_code != HTTPStatus.OK:
            raise TerminatingError(response.content)

        return response

    def replace_triple_data_in_repo(self, triple_data, content_type, auth=None):
        self.empty_repository(auth=auth)
        return self.add_triple_data_to_repo(triple_data, content_type, auth=auth)
