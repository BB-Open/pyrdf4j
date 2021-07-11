from requests.auth import HTTPBasicAuth
from pyrdf4j.constants import ADMIN_USER, ADMIN_PASS, VIEWER_PASS, VIEWER_USER, EDITOR_USER, \
    EDITOR_PASS

AUTH = {
    'viewer' : HTTPBasicAuth(VIEWER_USER, VIEWER_PASS),
#    'editor' : HTTPBasicAuth(EDITOR_USER, EDITOR_PASS),
    'admin' : HTTPBasicAuth(ADMIN_USER, ADMIN_PASS)
}

ACTORS = [
    'viewer',
#    'editor',
    'admin'
]


