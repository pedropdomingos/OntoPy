import json

class Mapping:

    def __init__(self,mapping_file_path):
        self.mapping_dict = self.__load_mapping_file(mapping_file_path)
        self.data_sources_ids = self.__get_data_sources_names()
        self.ontologies_classes_mapping = {}
        self.ontologies_predicates_triples = {}
        self.__get_ontologies_classes_mapping_and_subjects()

    def __load_mapping_file(self,mapping_file_path):
        with open(mapping_file_path, encoding='utf-8') as json_file:
            mapping_dict = json.load(json_file)
        return mapping_dict
    
    def __get_data_sources_names(self):
        return self.mapping_dict.keys()
    
    def __get_ontologies_classes_mapping_and_subjects(self):
        for data_source_id in self.data_sources_ids:
            self.ontologies_classes_mapping[data_source_id] = []
            self.ontologies_predicates_triples[data_source_id] = []
            for triple in self.__get_triples_list(data_source_id):
                self.ontologies_classes_mapping[data_source_id].append({})
                self.ontologies_predicates_triples[data_source_id].append({})
                self.ontologies_classes_mapping[data_source_id][-1][triple['subject']['data_source_attribute_name']] = triple['subject']['ontology_subject_name']
                self.ontologies_predicates_triples[data_source_id][-1][triple['subject']['data_source_attribute_name']] = triple['subject']['ontology_subject_name']
                for predicate in triple['predicates_and_objects']: 
                    self.ontologies_classes_mapping[data_source_id][-1][predicate['data_source_attribute_name']] = predicate['ontology_object_name']
                    self.ontologies_predicates_triples[data_source_id][-1][predicate['ontology_object_name']] = predicate['ontology_predicate_name']

    def __get_triples_list(self,data_source_id):
        return self.mapping_dict[data_source_id]['triples']