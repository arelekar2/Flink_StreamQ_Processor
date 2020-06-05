package dataSource

import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors.{Csv, Kafka, Rowtime, Schema}

object StreamTableConnector {

    def createTableSource(tableName: String, tableEnv: StreamTableEnvironment): Unit = {

        val derivedSchema = getSourceSchema(tableName)

        tableEnv.connect(
            new Kafka()
              .version("universal")
              .topic(tableName)
              .property("zookeeper.connect", "localhost:2181")
              .property("bootstrap.servers", "localhost:9092")
              .startFromEarliest())
          .withFormat(
              new Csv()
                .fieldDelimiter('|')
                .ignoreParseErrors()) // skip fields and rows with parse errors instead of failing; defaults to null
          .withSchema(derivedSchema)
          .inAppendMode()
          .createTemporaryTable(tableName)
    }


    def getSourceSchema(tableName: String): Schema = tableName match {
        case "customer" =>
            new Schema()
              .field("custkey", DataTypes.BIGINT())
              .field("name", DataTypes.STRING())
              .field("address", DataTypes.STRING())
              .field("nationkey", DataTypes.BIGINT())
              .field("phone", DataTypes.STRING())
              .field("acctbal", DataTypes.DOUBLE())
              .field("mktsegment", DataTypes.STRING())
              .field("comment", DataTypes.STRING())

        case "lineitem" =>
            new Schema()
              .field("orderKey", DataTypes.BIGINT())
              .field("partKey", DataTypes.BIGINT())
              .field("suppKey", DataTypes.BIGINT())
              .field("lineNumber", DataTypes.INT())
              .field("quantity", DataTypes.BIGINT())
              .field("extendedPrice", DataTypes.DOUBLE())
              .field("discount", DataTypes.DOUBLE())
              .field("tax", DataTypes.DOUBLE())
              .field("returnFlag", DataTypes.STRING())
              .field("lineStatus", DataTypes.STRING())
              .field("shipDate", DataTypes.STRING())
              .field("commitDate", DataTypes.STRING())
              .field("receiptDate", DataTypes.STRING())
              .field("shipInstruct", DataTypes.STRING())
              .field("shipMode", DataTypes.STRING())
              .field("comment", DataTypes.STRING())

        case "orders" =>
            new Schema()
              .field("orderkey", DataTypes.BIGINT())
              .field("custkey", DataTypes.BIGINT())
              .field("orderstatus", DataTypes.STRING())
              .field("totalprice", DataTypes.DOUBLE())
              .field("orderdate", DataTypes.STRING())
              .field("orderpriority", DataTypes.STRING())
              .field("clerk", DataTypes.STRING())
              .field("shippriority", DataTypes.INT())
              .field("comment", DataTypes.STRING())


    }

}
