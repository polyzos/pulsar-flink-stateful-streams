mvn clean package

flink run \
--class io.ipolyzos.compute.EnrichmentStream \
  target/pulsar-flink-stateful-streams-0.1.0.jar