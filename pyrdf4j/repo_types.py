"""
Repository configurations templates. Like the RDF4J_REST workbench offers.
"""

from pyrdf4j.errors import RepositoryTypeUnknown

from pathlib import Path

# The path to the repo_type_templates folder
TEMPLATE_FOLDER = Path(__file__).parent / 'repo_type_templates'

# The known repository types. Shamelessly stolen from RFD4J workbench
REPO_TYPES = [
'memory-rdfs-lucene',
'memory-spin-rdfs',
'native-lucene',
'native-shacl',
'native',
'memory-lucene',
'memory-rdfs',
'memory-spin',
'native-rdfs-dt',
'native-spin-rdfs-lucene',
'remote',
'memory-rdfs-dt',
'memory-shacl',
'memory',
'native-rdfs-lucene',
'native-spin-rdfs',
'sparql',
'memory-rdfs-legacy',
'memory-spin-rdfs-lucene',
'native-rdfs',
'native-spin',
]

# Default parameter values for the templates. Please consult the RDF4J_REST documentation on details to these parameters.
DEFAULTS = {
    'persist': 'false',
    'iterationCacheSyncThreshold': 10000,
    'evaluationStrategyFactory': 'org.eclipse.rdf4j.query.algebra.evaluation.impl.StrictEvaluationStrategyFactory',
    'syncDelay': 0,
    'tripleIndexes': 'spoc,posc',
    'queryLanguage' : 'SPARQL',
    'Sesame_server_location': 'http://example.org',
    'Remote_repository_ID': 'Test',
    'SPARQL_query_endpoint': 'http://example.org',
    'SPARQL_update_endpoint': 'http://example.org',
}


def repo_config_factory(repo_type, repo_id, repo_label, **kwargs):
    """
    Constructs a repository configuration in form of a TTL structure utilizing the TTL templates from ./repo_types_template.
    """
    # Check if the repo_type is a known template
    if repo_type not in REPO_TYPES:
        raise RepositoryTypeUnknown
    # Get the path to the template
    template_path = TEMPLATE_FOLDER / '{}{}'.format(repo_type,'.ttl')
    # Open the template file and read it
    with open(template_path) as template_file:
        template = template_file.read()

    # get the default values for the template
    params = DEFAULTS
    # Overwrite them with the given kwargs
    params.update(kwargs)
    # Fill the params in the template
    ttl = template.format(repo_id=repo_id.replace('-', '_'), repo_label=repo_label, **params)
    # return the final TTL
    return ttl
