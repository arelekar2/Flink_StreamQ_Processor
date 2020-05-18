package source

import java.util.Properties
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object ConsumerFactory {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("zookeeper.connect", "localhost:2181")
    
    
    def apply(topic: String): FlinkKafkaConsumer[String] = topic match {
        case "customer" | "lineitem" | "nation" | "orders" | "part" | "partsupp" | "region" | "supplier" =>
            new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), properties)
        
    }
    
}
