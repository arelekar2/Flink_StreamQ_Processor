package source

import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors.{Csv, Kafka, Schema}

object TableConnector {
    
    def createTable(tableName: String, tableEnv: StreamTableEnvironment): Unit = {
        
        val derivedSchema = getSchema(tableName)
        
        tableEnv.connect(
            new Kafka()
              .version("universal")
              .topic("lineitem")
              .property("zookeeper.connect", "localhost:2181")
              .property("bootstrap.servers", "localhost:9092")
              .startFromEarliest())
          .withFormat(
              new Csv()
                .fieldDelimiter('|')
                .ignoreParseErrors()) // skip fields and rows with parse errors instead of failing; defaults to null
          .withSchema(derivedSchema)
          .createTemporaryTable(tableName)
    }
    
    
    def getSchema(tableName: String): Schema = tableName match {
        case "lineitem" =>
            new Schema()
              .field("orderKey", DataTypes.BIGINT())
              .field("partKey", DataTypes.BIGINT())
              .field("suppKey", DataTypes.BIGINT())
              .field("lineNumber", DataTypes.INT())
              .field("quantity", DataTypes.DOUBLE())
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
              .field("Comment", DataTypes.STRING())
        
    }
    
}
