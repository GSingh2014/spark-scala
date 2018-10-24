## Build
mvn clean compile package

## Run
spark-submit.cmd --class com.sparkScala.Ka
fkaStreamingProducer --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.2 .\target\sparkScalaKafkaStreaming-1.0-S
NAPSHOT.jar
