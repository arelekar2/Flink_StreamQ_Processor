import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Table, Tumble}
import org.apache.flink.types.Row
import source.{ConsumerFactory, TableConnector}

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
        TableConnector.createTable(tableName, tableEnv)
        
        //        val lineitemConsumer = ConsumerFactory("lineitem")
        //        lineitemConsumer.setStartFromEarliest()
        //
        //        val lineitemStream: DataStream[LineItem] = env.addSource(lineitemConsumer)
        //          .map { line => line.split("\\|").map(_.trim).toList }
        //          .map { l => LineItem(l.head.toLong, l(1).toLong, l(2).toLong, l(3).toInt, l(4).toDouble, l(5).toDouble, l(6).toDouble, l(7).toDouble, l(8), l(9), l(10), l(11), l(12), l(13), l(14), l(15)) }
        //
        
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
              'orderKey.count as 'count_order
          )
        
        result.toRetractStream[Row]
          .filter(row => row._1)
          .print()
          .setParallelism(1)
        
        // write output to csv
        //        result.toAppendStream[Row].writeAsCsv("file:///home/hadoop/a.txt").setParallelism(1)
        
        env.execute("TPCH Query1")
        
        
    }
    
    
    // *************************************************************************
    //     USER DATA TYPES: maybe needed if DataStream is used
    // *************************************************************************
    
    //    case class LineItem(orderKey: Long, partKey: Long, suppKey: Long, lineNumber: Int, quantity: Double,
    //                        extendedPrice: Double, discount: Double, tax: Double, returnFlag: String, lineStatus: String,
    //                        shipDate: String, commitDate: String, receiptDate: String, shipInstruct: String,
    //                        shipMode: String, Comment: String)
    
    
    // *************************************************************************
    //     EXPLAIN PLAN - for optimization
    // *************************************************************************
    //    val explanation: String = tEnv.explain(table)
    //    println(explanation)
    
}
