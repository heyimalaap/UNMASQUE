from typing import List, Tuple, Dict
import csv

from mysite.unmasque.refactored.util.common_queries import alter_table_rename_to, create_table_as_select_star_from, get_restore_name, get_star, get_tabname_1
from .abstract.MinimizerBase import Minimizer

# Queries
def Q_get_value_freq(table: str, attribute: str):
    return f"SELECT {attribute}, COUNT(*) FROM {table} GROUP BY {attribute};"

def Q_keep_only_value(table: str, attribute: str, value: str | int):
    if type(value) is not int:
        value = f"'{value}'"
    return f"DELETE FROM {table} WHERE {attribute} != {value}"



class HavingMinimizer(Minimizer):

    def __init__(self, connectionHelper,
                 core_relations, core_sizes,
                 sampling_status):
        super().__init__(connectionHelper, core_relations, core_sizes, "Having_Minimizer")
        self.cs2_passed = sampling_status

        self.table_attributes_map: dict[str, list[str]] = dict()

    def extract_params_from_args(self, args):
        return args[0]

    def doActualJob(self, args):
        query = self.extract_params_from_args(args)

        self.initialize_minimizer()
        check = self.run_minimizer(query)
        self.finalize_minimizer()

        return check

    def initialize_minimizer(self):
        for table in self.core_relations:
            table_temp_name = self.to_temp_table_name(table)
            self.connectionHelper.execute_sql([alter_table_rename_to(table, table_temp_name)])
            self.connectionHelper.execute_sql([create_table_as_select_star_from(table, table_temp_name)])

    def finalize_minimizer(self):
        pass


    def get_schema_graph(self):
        res_path = self.connectionHelper.config.base_path
        pkfk_file_path = (res_path / self.connectionHelper.config.pkfk).resolve()
        with open(pkfk_file_path, 'rt') as f:
            data = csv.reader(f)
            raw_schema = list(data)[1:]

        schema_graph = []
        
        primary_keys = [(entry[0], entry[1]) for entry in raw_schema if entry[2] == 'y']
        for entry in raw_schema:
            continue

        return schema_graph
    
    def to_temp_table_name(self, table_name: str):
        return get_tabname_1(table_name) if self.cs2_passed else get_restore_name(table_name)


    def from_catalog_where(self):
        return " FROM information_schema.columns " + \
            "WHERE table_schema = '" + self.connectionHelper.config.schema + "' and TABLE_CATALOG= '" \
            + self.connectionHelper.db + "' "

    def get_attributes(self, table_name: str) -> list[str]:
        """Queries the database for the attributes of a table
        
        Args:
            table_name (str): Name of the table whoes attributes we want
        """

        # Cache results to reduce database overhead
        if self.table_attributes_map.get(table_name):
            return self.table_attributes_map[table_name]

        res, _ = self.connectionHelper.execute_sql_fetchall("select column_name " + self.from_catalog_where() +" and table_name = '" + table_name + "';")
        self.table_attributes_map[table_name] = [row[0] for row in res]
        
        return self.table_attributes_map[table_name]

    def get_frequency_of_values(self, table_name: str, except_in: List[str] = []) -> Dict[Tuple[str, str | int], int]:
        """Returns a dictionary where the key is a tuple of the form (Attribute, Value) and the value is the frequency"""

        freq: Dict[Tuple[str, str | int], int] = dict()

        for attr in self.get_attributes(table_name):
            if attr in except_in:
                continue # we ignore this attribute

            res, _ = self.connectionHelper.execute_sql_fetchall(Q_get_value_freq(table_name, attr))
            for row in res:
                v, f = row[0], row[1]
                freq[(attr, v)] = f

        return freq

    def get_frequency_sorted_attr_value(self, table_name: str, except_in: List[str] = []) -> Tuple[List[Tuple[str, str | int]], Dict[Tuple[str, str | int], int]]:
        """Returns a list of (Attribute, Value) tuples that are sorted in the order of frequency"""

        freq = self.get_frequency_of_values(table_name, except_in)
        result = list(freq.keys())
        result.sort(key=lambda x: freq[x], reverse=True)

        return result, freq

    def begin_transaction(self):
        return super().begin_transaction()

    def commit_transaction(self):
        return super().commit_transaction()

    def rollback_transaction(self):
        self.connectionHelper.execute_sql(["ROLLBACK;"])

    def run_minimizer(self, query: str) -> bool:
        is_minimized = False
        minimized: Dict[str, List[str]] = dict()
        for table in self.core_relations:
            minimized[table] = []

        while not is_minimized:
            print(minimized)
            breakpoint()
            for table in self.core_relations:
                print(f"[+] Minimizing table {table}")
                sorted_attrib_val, _ = self.get_frequency_sorted_attr_value(table, minimized[table])


                for attrib, value in sorted_attrib_val:
                    print(f'\t[*] Trying {attrib} = {value}')
                    self.begin_transaction()
                    self.connectionHelper.execute_sql([Q_keep_only_value(table, attrib, value)])
                    res, _ = self.connectionHelper.execute_sql_fetchall(query)

                    if len(res) == 0:
                        # if the result was empty, then roll back!
                        self.rollback_transaction()
                        continue
                    
                    minimized[table].append(attrib)
                    self.commit_transaction()

        breakpoint()

        return True
