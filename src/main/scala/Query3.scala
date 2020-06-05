import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.types.Row
import dataSource.StreamTableConnector

object Query3 {
    val customerTableName: String = "customer"
    val lineitemTableName: String = "lineitem"
    val ordersTableName: String = "orders"

    def main(args: Array[String]): Unit = {

        // get stream execution environment
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        // get environment settings for flink planner in streaming mode
        val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()

        // create a TableEnvironment
        val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

        // create temporary flink table
        List(customerTableName, lineitemTableName, ordersTableName)
          .foreach(tableName => StreamTableConnector.createTableSource(tableName, tableEnv))


        val segment: String = "AUTOMOBILE"

        val filteredCustomer: Table = tableEnv.from("customer")
          .where('mktsegment === segment)
          .select('custkey as 'c_custkey)

        val filteredLineitem: Table = tableEnv.from("lineitem")
          .where('shipDate.toDate > "1991-11-26".toDate)
          .select('orderKey as 'l_orderkey, 'extendedPrice, 'discount)

        val filteredOrders: Table = tableEnv.from("orders")
          .where('orderdate.toDate < "1998-12-01".toDate)
          .select('orderkey as 'o_orderkey, 'custkey as 'o_custkey, 'orderdate, 'shippriority)

        val result: Table = filteredCustomer.join(filteredOrders).where('c_custkey === 'o_custkey)
          .join(filteredLineitem).where('l_orderkey === 'o_orderkey)
          .groupBy('l_orderkey, 'orderdate, 'shippriority)
          .select('l_orderkey, ('extendedPrice * (1.toExpr - 'discount)).sum as 'revenue, 'orderdate, 'shippriority)


        result.toRetractStream[Row]
          .filter(row => row._1)
          .map(row => row._2)
          .print()
          .setParallelism(1)

        // write output to csv
        //        result.toRetractStream[Row]
        //          .filter(row => row._1)
        //          .map(row => row._2)
        //          .writeAsCsv("../resources/output/Query3.csv")
        //          .setParallelism(1)

        env.execute("TPCH Query3")


    }

}
