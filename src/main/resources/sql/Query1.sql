select
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
from
    lineitem
where
    l_shipdate <=  "1998-12-01"
group by
    l_returnflag, l_linestatus
order by
    l_returnflag, l_linestatus;


-- Sample Output (When DELTA == 90)
-- L_RETURNFLAG | L_LINESTATUS | SUM_QTY     | SUM_BASE_PRICE | SUM_DISC_PRICE | SUM_CHARGE     | AVG_QTY | AVG_PRICE | AVG_DISC | COUNT_ORDER
-- A            | F            | 37734107.00 | 56586554400.73 | 53758257134.87 | 55909065222.83 | 25.52   | 38273.13  | 0.05     | 1478493