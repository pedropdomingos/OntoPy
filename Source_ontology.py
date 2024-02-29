from owlready2 import *

class Source_ontology:
    
    def __new__(cls,ontology_file_path):
        source_ontology = cls.__load_ontology_file(ontology_file_path)
        source_ontology.name = source_ontology.base_iri
        return source_ontology

    @classmethod
    def __load_ontology_file(cls,ontology_file_path):
        return get_ontology(ontology_file_path).load()