import csv
from error import *

class Database:
    def __init__(self, metadata_file_path):
        self.tableInfo = {}
        self.tables = {}
        self.fill_table_info(metadata_file_path)
        self.fill_tables()    
    
    def parse_metadata(self, data):
        for line in data:
            if line[len(line)-1] == '\n':
                line = line[:-1]

            if line == '<begin_table>':
                table = []
                table_name = ''
            elif line == '<end_table>':
                self.tableInfo[table_name] = table
            elif len(table_name) == 0:
                table_name = line
            else:
                table.append(line)
                
    def parse_table(self, data, table_name):
        col_names = self.tableInfo[table_name]
        cur_table = []
        for row in data:
            if len(row) != len(col_names):
                show_error(UPDATE_METADATA_ERROR)
            table_row = []
            for item in row:
                try:
                    table_row.append(int(item))
                except:
                    show_error(NUMBER_ALLOWED_ERROR)
            cur_table.append(table_row)
        return cur_table

    
    def get_table(self, table_name):
        table_path = 'files/' + table_name + '.csv'
        try:
            with open(table_path, 'r') as table:
                table_obj = csv.reader(table)
                cur_table = self.parse_table(table_obj, table_name)
        except Exception as e:
            show_error(FILE_OPEN_ERROR.format("Table"), e)
        return cur_table
        

    def fill_table_info(self, metadata_file_path):
        try:
            with open(metadata_file_path, 'r') as metadata:
                data = metadata.readlines()
                self.parse_metadata(data)
        except Exception as e:
            show_error(FILE_OPEN_ERROR.format("Metadata"), e)
            
    def fill_tables(self):
        for table_name in self.tableInfo:
            self.tables[table_name] = self.get_table(table_name)
