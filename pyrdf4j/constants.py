"""Constants"""

RDF4J_BASE = 'http://192.168.122.193:8080/rdf4j-server/'

ADMIN_USER = 'admin'
ADMIN_PASS = 'pw1'

EDITOR_USER = 'editor'
EDITOR_PASS = 'pw2'

VIEWER_USER = 'viewer'
VIEWER_PASS = 'pw3'

DEFAULT_QUERY_MIME_TYPE = 'application/sparql-query'
# used for generating RDF-DATA by CONSTRUCT Statement
DEFAULT_RESPONSE_TRIPLE_MIME_TYPE = 'application/rdf+xml'
# used for getting query results by SELECT Statement
DEFAULT_QUERY_RESPONSE_MIME_TYPE = 'application/sparql-results+json'
DEFAULT_CONTENT_TYPE = 'application/x-turtle'

DEFAULT_CHARSET = 'utf-8'
