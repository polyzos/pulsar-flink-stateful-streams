mvn clean package

docker cp \
  target/pulsar-flink-stateful-streams-0.1.0.jar \
  pulsar-flink-stateful-streams_taskmanager_1:opt/flink/job.jar

docker exec -it pulsar-flink-stateful-streams_taskmanager_1 ./bin/flink run \
--class io.ipolyzos.compute.EnrichmentStream \
  job.jar