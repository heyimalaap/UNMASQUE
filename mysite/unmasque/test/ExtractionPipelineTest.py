import unittest

from mysite.unmasque.refactored.ConnectionHelper import ConnectionHelper
from mysite.unmasque.refactored.executable import Executable
from mysite.unmasque.refactored.util.utils import isQ_result_empty
from mysite.unmasque.src.core import ExtractionPipeLine
from mysite.unmasque.test import tpchSettings, queries


class MyTestCase(unittest.TestCase):
    conn = ConnectionHelper("tpch", "postgres", "postgres", "5432", "localhost")

    def test_for_tpch_query1(self):
        q_key = 'tpch_query1'
        self.conn.connectUsingParams()
        query = queries.queries_dict[q_key]
        eq, _ = ExtractionPipeLine.extract(self.conn, query)
        self.conn.closeConnection()
        self.assertTrue(eq is not None)
        print(eq)

    def test_all_sample_queries(self):
        Q_keys = queries.queries_dict.keys()
        f = open("experiment_results.txt", "w")
        q_no = 1
        for q_key in Q_keys:
            self.conn.connectUsingParams()
            query = queries.queries_dict[q_key]
            app = Executable(self.conn)
            result = app.doJob(query)
            if isQ_result_empty(result):
                print("Hidden query doesn't produce a populated result. It is beyond the scope of Unmasque..skipping "
                      "query!")
                continue
            f.write("\n" + str(q_no) + ":")
            f.write("\tHidden Query:\n")
            f.write(query)
            eq, _ = ExtractionPipeLine.extract(self.conn, query)
            f.write("\n*** Extracted Query:\n")
            if eq is None:
                f.write("Extraction Failed!")
            else:
                f.write(eq)
            f.write("\n---------------------------------------\n")
            q_no += 1
            self.conn.closeConnection()

        f.close()


if __name__ == '__main__':
    unittest.main()
