#FROM apachepulsar/pulsar:2.8.0 as pulsar
FROM apachepulsar/pulsar:2.9.1 as pulsar

ENV PULSAR_HOME=/pulsar

COPY conf/standalone.conf $PULSAR_HOME/conf/standalone.conf