

class SPARQL:
    """
    API to the SPARQL Endpoint of a repository
    """

    def __init__(self, uri):
        self.sparql = SPARQLWrapper2(uri)

    def exists(self, _URI):
        """
        :param _URI:
        :return:
        """
        self.sparql.setQuery("""
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            SELECT ?s
            WHERE { ?s ?p ?o }
                    """)
        results = self.sparql.query()
        if results:
            return True

    def insert(self, triple):
        """
        :param triple:
        :return:
        """
        queryString = """INSERT DATA
           {{ GRAPH <http://example.com/> {{ {s} {p} {o} }} }}"""

        self.sparql.setQuery(
            queryString.format(s=triple.s, o=triple.o, p=triple.p))
        self.sparql.method = 'POST'
        self.sparql.query()

    def query(self, queryString):
        """
        :param queryString:
        :return:
        """
        self.sparql.setQuery(queryString)
        results = self.sparql.query()
        return results
