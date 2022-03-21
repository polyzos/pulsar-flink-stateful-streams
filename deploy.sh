mvn clean package

flink run \
--class io.ipolyzos.compute.LookupStream \
  target/pulsar-flink-stateful-streams-0.1.0.jar