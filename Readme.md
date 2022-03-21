<p align="center">
    <img src="images/pf1.png" width="600" height="400">
</p>


WIP:
- [ ] Optimize to backpressure, buffers, checkpoint intervals and wm intervals for larger state 
- [ ] User RocksDB API to demonstrate what gets written and how
- [ ] Use time based joins for session windows and add time constraints

Scenario 1
==========
<p align="center">
    <img src="images/pf2.png" width="800" height="300">
</p>

Scenario 2
==========
<p align="center">
    <img src="images/pf3.png" width="800" height="300">
</p>

Setup a Pulsar Cluster
======================
```shell
docker run -rm -it --name pulsar \
-p 6650:6650  -p 8080:8080 \
--mount source=pulsardata,target=/pulsar/data \
--mount source=pulsarconf,target=/pulsar/conf \
apachepulsar/pulsar:2.9.1 \
bin/pulsar standalone
```

Setup Pulsar Logical Components
===============================
Go into your container
```shell
docker exec -it pulsar bash
```

and run the following commands
1. Create topics
```shell
bin/pulsar-admin topics create-partitioned-topic -p 1 persistent://public/default/orders
bin/pulsar-admin topics create-partitioned-topic -p 1 persistent://public/default/users
bin/pulsar-admin topics create-partitioned-topic -p 1 persistent://public/default/items

bin/pulsar-admin topics create-partitioned-topic -p 1 persistent://public/default/view_events
bin/pulsar-admin topics create-partitioned-topic -p 1 persistent://public/default/purchase_events
bin/pulsar-admin topics create-partitioned-topic -p 1 persistent://public/default/cart_events

bin/pulsar-admin topics list public/default
```

2. Set infinite Retention
```shell
bin/pulsar-admin topics set-retention -s -1 -t -1 persistent://public/default/users
bin/pulsar-admin topics set-retention -s -1 -t -1 persistent://public/default/items

bin/pulsar-admin topics get-retention persistent://public/default/users
bin/pulsar-admin topics get-retention persistent://public/default/items
```

Start a Flink Cluster
=====================
```shell
start-cluster
```

Deploy the Flink Job
```shell
./deploy.sh
```

Monitor Flink logs
==================
Tail the logs
```shell
tail -f log/flink-*-taskexecutor-*
```

The original Datasets can be found on the following links:
- https://www.kaggle.com/datasets/alaasedeeq/dsc1069?select=dsv1069_events.csv
- https://www.kaggle.com/datasets/mkechinov/ecommerce-events-history-in-cosmetics-shop