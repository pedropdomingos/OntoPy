import os
import pandas as pd

class Data_sources:

    def __init__(self, mapping_file,dataframes):
        self.dataframes = {}
        if dataframes == None:
            self.__load_data_sources(mapping_file)
        else:
            self.dataframes = dataframes

    def __load_data_sources(self, mapping_file):
        for data_source_id in mapping_file.data_sources_ids:
            self.dataframes[data_source_id] = self.__read_data_source(data_source_id,mapping_file)

    def __read_data_source(self,data_source_id,mapping_file):
        data_source_path = self.__get_data_source_path(data_source_id,mapping_file) 
        data_source_separator = self.__get_data_source_separator(data_source_id,mapping_file)
        data_source_decimal = self.__get_data_source_decimal(data_source_id,mapping_file)
        if data_source_path == "Dataframe":
            return globals()[data_source_id]
        else:
            return self.__read_file_system(data_source_path,
                                           data_source_separator,
                                           data_source_decimal)

    def __get_data_source_path(self,data_source_id,mapping_file):
        return mapping_file.mapping_dict[data_source_id]['data_source_path']

    def __get_data_source_separator(self,data_source_id,mapping_file):
        return mapping_file.mapping_dict[data_source_id]['separator']
    
    def __get_data_source_decimal(self,data_source_id,mapping_file):
        return mapping_file.mapping_dict[data_source_id]['decimal']    

    def __read_file_system(self,data_source_path,data_source_separator,data_source_decimal):
        file_system_functions = {
            '.csv': self.__read_csv_file,
            '.parquet': self.__read_parquet_file,
            '.xlsx': self.__read_xlsx_file,
        }
        data_source_extension = self.__get_data_source_extension(data_source_path)
        return file_system_functions[data_source_extension](data_source_path,
                                                            data_source_separator,
                                                            data_source_decimal)
    
    def __read_csv_file(self,data_source_path,data_source_separator,data_source_decimal):
        return pd.read_csv(data_source_path,
                           sep=data_source_separator,
                           decimal=data_source_decimal)

    def __read_parquet_file(self,data_source_path,data_source_separator,data_source_decimal):
        return pd.read_parquet(data_source_path,engine='pyarrow')
    
    def __read_xlsx_file(self,data_source_path,data_source_separator,data_source_decimal):
        return pd.read_excel(data_source_path)

    def __get_data_source_extension(self,data_source_path):
        return os.path.splitext(data_source_path)[1]    