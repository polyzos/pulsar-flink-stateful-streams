#mvn clean package

docker cp \
  target/pulsar-flink-stateful-streams-0.1.0.jar \
  pulsar-flink-stateful-streams-taskmanager-1:opt/flink/job.jar

# To run v1 of the code
docker exec -it pulsar-flink-stateful-streams-taskmanager-1 ./bin/flink run \
--class io.ipolyzos.compute.v1.EnrichmentStream \
  job.jar

# To run v2 of the code
#docker exec -it pulsar-flink-stateful-streams_taskmanager_1 ./bin/flink run \
#--class io.ipolyzos.compute.v2.EnrichmentStream \
#  job.jar
#
## To run v3 of the code
#docker exec -it pulsar-flink-stateful-streams_taskmanager_1 ./bin/flink run \
#--class io.ipolyzos.compute.v3.EnrichmentStream \
#  job.jar
#
## To run v4 of the code
#docker exec -it pulsar-flink-stateful-streams_taskmanager_1 ./bin/flink run \
#--class io.ipolyzos.compute.v4.EnrichmentStream \
#  job.jar