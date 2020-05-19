# Flink_StreamQ_Processor
Stream Query Processor implemented using Apache Flink for TPC-H Query 1 & 3

## Requirements
- sbt v1.3.10
- Scala v2.12.11
- Apache Flink v1.10.0
- Apache Kafka v2.5.0
- Apache ZooKeeper v3.5.7

## How to run:
1. create topics:  
  `kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic customer`  
  `kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic lineitem`  
  `kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic orders`
  
2. go to project dir and Start the Stream query processor:  
`sbt run target/scala-2.12/stream_query_processor_2.12-1.0.jar`
  
3. go to `src/main/resource` folder and run the script to start stream data:  
`bash csvToKafka.sh`


   
> Project status (WIP):  
>- Query1: implemented
>- Query3: next