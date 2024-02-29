from owlready2 import *

class Materialized_ontology:

    def __new__(cls,source_ontology,source_ontology_file_name):
        materialized_ontology = cls.__generate_materialized_ontology(source_ontology,
                                                           source_ontology_file_name)
        return materialized_ontology
    
    @classmethod
    def __generate_materialized_ontology(cls,source_ontology,source_ontology_file_name):
        materialized_ontology = get_ontology(source_ontology.base_iri)
        materialized_ontology.imported_ontologies.append(source_ontology)        
        return materialized_ontology 