from Source_ontology import Source_ontology
from Materialized_ontology import Materialized_ontology
from Mapping import Mapping
from Data_sources import Data_sources
from Data_sources_dicts import Data_sources_dicts

from owlready2 import *
import itertools
import pandas as pd
import math


class OntoPy:

    def __init__(self,ontology_file_path, mapping_file_path, dataframes=None):
        self.source_ontology_file_name = self.__return_source_ontology_file_name(ontology_file_path)
        self.source_ontology = Source_ontology(ontology_file_path)
        self.materialized_ontology = Materialized_ontology(self.source_ontology,
                                                 self.source_ontology_file_name)
        self.mapping_file = Mapping(mapping_file_path)
        self.data_sources = Data_sources(self.mapping_file, dataframes)
        self.data_sources_dicts = Data_sources_dicts(self.mapping_file,
                                                     self.data_sources.dataframes)
        self.__materialize_ontology()
        try:
            self.materialized_ontology.save(file='temp.owl')
        except:
            raise SubjectError()
    

    def __return_source_ontology_file_name(self, ontology_file_path):
        return os.path.basename(ontology_file_path)

    def __materialize_ontology(self):
        with self.materialized_ontology:
            list(itertools.starmap(self.__populate_ontology, self.__generate_arguments()))
        
    def __generate_arguments(self):
        return [(data_source_id, data_source_dict_index, subject_instance) for data_source_id in self.mapping_file.data_sources_ids 
                                                                           for data_source_dict_index in range(len(self.data_sources_dicts[data_source_id])) 
                                                                           for subject_instance in self.data_sources_dicts[data_source_id][data_source_dict_index]]

    def __populate_ontology(self,data_source_id, data_source_dict_index, subject_instance):
        data_triple = self.__generate_data_triple(data_source_id, data_source_dict_index, subject_instance)
        class_identifier = self.__get_class_identifier(data_source_id, data_source_dict_index)
        ontology_instance = self.materialized_ontology[class_identifier](str(subject_instance), **data_triple)

    def __generate_data_triple(self, data_source_id, data_source_dict_index, subject_instance):
        data_source_dict = self.mapping_file.ontologies_predicates_triples[data_source_id][data_source_dict_index]
        args = [(object_class, object_instance) for object_class, object_instance in self.data_sources_dicts[data_source_id]
                                                                                                            [data_source_dict_index]
                                                                                                            [subject_instance].items()
                                                      if self.__not_nan(object_instance)]

        return dict(itertools.starmap(
            lambda object_class, object_instance: (
                data_source_dict[object_class],
                self.materialized_ontology[object_class](str(object_instance))
                if self.__predicate_has_instance(data_source_dict,object_class) else None
            ),
            args
        ))

    def __get_class_identifier(self,data_source_id,data_source_dict_index):
        return list(self.mapping_file.ontologies_predicates_triples[data_source_id][data_source_dict_index].values())[0]
    
    def __not_nan(self,value):
        if type(value) != float:
            return True
        else:
            return not math.isnan(float(value))
        
    def __predicate_has_instance(self,data_source_dict,predicate_instance):
        return bool(predicate_instance in data_source_dict and callable(self.source_ontology[data_source_dict[predicate_instance]]))

    def sparql_query(self, user_query):
        
        def return_colums_names(results_list):
            columns_list = []
            for instance in results_list[0]:
                columns_list.append(instance.is_a[0].name)
            return columns_list
        
        def return_only_instances(results_list):
            for row_index in range(len(results_list)):
                for instance_index in range(len(results_list[row_index])):
                    instance = results_list[row_index][instance_index].name
                    results_list[row_index][instance_index] = instance
            return results_list       
        
        results_list = list(default_world.sparql(user_query))
        if not results_list: raise EmptyList()
        print(results_list)

        columns_list = return_colums_names(results_list)
        results_dataframe = pd.DataFrame(return_only_instances(results_list),
                                         columns=columns_list)
        return results_dataframe


class SubjectError(Exception):
    def __init__(self, message="SubjectError: invalid subject instances"):
        super().__init__(message)
                            

class EmptyList(Exception):
    def __init__(self, message="EmptyList: no results from SPARQL query"):
        super().__init__(message)
        
