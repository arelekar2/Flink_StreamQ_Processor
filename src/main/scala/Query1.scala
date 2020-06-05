import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.types.Row
import dataSource.StreamTableConnector

object Query1 {
    val tableName: String = "lineitem"

    def main(args: Array[String]): Unit = {

        // get stream execution environment
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        // get environment settings for flink planner in streaming mode
        val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()

        // create a TableEnvironment
        val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

        // create temporary flink table
        StreamTableConnector.createTableSource(tableName, tableEnv)

        val result: Table = tableEnv.from("lineitem")
          .where('shipDate.toDate <= "1998-12-01".toDate)
          .groupBy('returnFlag, 'lineStatus)
          .select(
              'returnFlag,
              'lineStatus,
              'quantity.sum as 'sum_qty,
              'extendedPrice.sum as 'sum_base_price,
              ('extendedPrice * (1.toExpr - 'discount)).sum as 'sum_disc_price,
              ('extendedPrice * (1.toExpr - 'discount) * (1.toExpr + 'tax)).sum as 'sum_charge,
              'quantity.avg as 'avg_qty,
              'extendedPrice.avg as 'avg_price,
              'discount.avg as 'avg_disc,
              'orderKey.count as 'count_order)

        result.toRetractStream[Row]
          .filter(row => row._1)
          .map(row => row._2)
          .print()
          .setParallelism(1)

        // write output to csv
        //        result.toRetractStream[Row]
        //          .filter(row => row._1)
        //          .map(row => row._2)
        //          .writeAsCsv("../resources/output/Query1.csv")
        //          .setParallelism(1)

        env.execute("TPCH Query1")


    }


}
