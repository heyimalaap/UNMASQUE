import sys
import unittest

sys.path.append("../../../")

from mysite.unmasque.test.util.BaseTestCase import BaseTestCase
from mysite.unmasque.refactored.from_clause import FromClause
from mysite.unmasque.test.util import queries


class MyTestCase(BaseTestCase):

    def test_like_tpchq1(self):
        query = queries.tpch_query1
        self.conn.connectUsingParams()
        self.assertTrue(self.conn.conn is not None)
        fc = FromClause(self.conn)
        rels = fc.doJob(query, "error")
        print("Rels", rels)
        self.assertEqual(len(rels), 1)
        self.assertTrue('lineitem' in rels)
        self.conn.closeConnection()

    def test_like_tpchq3(self):
        query = queries.tpch_query3
        self.conn.connectUsingParams()
        self.assertTrue(self.conn.conn is not None)
        fc = FromClause(self.conn)
        rels = fc.doJob(query, "error")
        self.assertEqual(len(rels), 3)
        self.assertTrue('lineitem' in rels)
        self.assertTrue('customer' in rels)
        self.assertTrue('orders' in rels)
        self.conn.closeConnection()


if __name__ == '__main__':
    unittest.main()
