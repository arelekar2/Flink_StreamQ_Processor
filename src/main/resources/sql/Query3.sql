select
    l_orderkey,
    sum(l_extendedprice*(1-l_discount)) as revenue,
    o_orderdate,
    o_shippriority
from
    customer,
    orders,
    lineitem
where
    c_mktsegment = 'AUTOMOBILE' and
     c_custkey = o_custkey and
    l_orderkey = o_orderkey and
    o_orderdate < '1998-12-01' and
     l_shipdate > '1991-11-26'
group by
    l_orderkey, o_orderdate, o_shippriority;


-- Sample Output (When SEGMENT = BUILDING and DATE = 1995-03-15)
-- L_ORDERKEY | REVENUE   | O_ORDERDATE | O_SHIPPRIORITY
-- 2456423    | 406181.01 | 1995-03-05  | 0