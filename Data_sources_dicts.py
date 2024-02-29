class Data_sources_dicts:

    def __new__(cls, mapping_file, dataframes):
        data_source_dicts = cls.__generate_data_sources_dicts(mapping_file, dataframes)
        return data_source_dicts

    @classmethod
    def __generate_data_sources_dicts(cls,mapping_file, dataframes):
        data_source_dicts = {}
        for data_source_id in mapping_file.data_sources_ids:
            data_source_dicts[data_source_id] = []
            data_source = dataframes[data_source_id]
            ontology_mapping = mapping_file.ontologies_classes_mapping[data_source_id]
            for data_source_mapping in ontology_mapping:
                atributes_list = list(data_source_mapping.values())
                selected_data_source = data_source[data_source_mapping.keys()]
                selected_data_source = selected_data_source.rename(columns=data_source_mapping)
                selected_data_source = selected_data_source[atributes_list].set_index(atributes_list[0])
                selected_data_source = selected_data_source[~selected_data_source.index.duplicated(keep='last')]
                data_source_dicts[data_source_id].append(selected_data_source.to_dict('index'))
        return data_source_dicts

