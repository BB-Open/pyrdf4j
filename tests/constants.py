from requests.auth import HTTPBasicAuth
from pyrdf4j.constants import ADMIN_USER, ADMIN_PASS, VIEWER_PASS, VIEWER_USER

AUTH = {
    'viewer' : HTTPBasicAuth(VIEWER_USER, VIEWER_PASS),
    'admin' : HTTPBasicAuth(ADMIN_USER, ADMIN_PASS)
}

ACTORS = [
    'viewer',
    'admin'
]


RDF4J_BASE_TEST = 'http://192.168.1.71:8080/rdf4j-server/'
