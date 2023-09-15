from .core import ExtractionPipeLine

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    hq = "select c_mktsegment, l_orderkey, sum(l_extendedprice) as revenue, " \
         "o_orderdate, o_shippriority from customer, orders, lineitem where c_custkey = o_custkey " \
         "and l_orderkey = o_orderkey and o_orderdate > date '1995-10-11' " \
         "group by l_orderkey, o_orderdate, o_shippriority, c_mktsegment limit 4;"

    hq = "select l_orderkey as orderkey, sum(l_extendedprice * (1-l_discount)) as revenue, o_orderdate " \
         "as orderdate, " \
       "o_shippriority as " \
       "shippriority from customer, orders, " \
       "lineitem where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate " \
       "< '1995-03-15' and l_shipdate > '1995-03-15' group by l_orderkey, o_orderdate, o_shippriority order by revenue " \
       "desc, o_orderdate, l_orderkey limit 10;"

    eq, time = ExtractionPipeLine.extract(hq)

    print("=========== Extracted Query =============")
    print(eq)
    time.print()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
