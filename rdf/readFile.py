from rdflib import Graph
from rdflib.namespace import Namespace, NamespaceManager

if __name__ == '__main__':
    #ns = Namespace('http://purl.org/dc/terms/')
    namespace_manager = NamespaceManager(Graph())
    namespace_manager.bind('dct', Namespace('http://purl.org/dc/terms/'), override=False)
    namespace_manager.bind('dc', Namespace('http://purl.org/dc/elements/1.1/'), override=False)
    namespace_manager.bind('bibo', Namespace('http://purl.org/ontology/bibo/'), override=False)
    namespace_manager.bind('rdau', Namespace('http://rdaregistry.info/Elements/u/'), override=False)
    namespace_manager.bind('bf', Namespace('http://bibframe.org/vocab/'), override=False)

    g = Graph()
    g.namespace_manager = namespace_manager
    #g.parse("/usr/local/swissbib/mfWorkflows/src/main/resources/gh/mflearning/december/out.json", format="json-ld")
    g.load("/usr/local/swissbib/mfWorkflows/src/main/resources/gh/mflearning/december/rdfdump_20171226_074635644_XO_noType", format="json-ld")


    #file = open ("test.txt","w")
    g.serialize(destination="test.ttl",format='ttl', indent=4)