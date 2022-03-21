package io.ipolyzos.compute;

import io.ipolyzos.compute.handlers.ItemLookupHandler;
import io.ipolyzos.compute.handlers.UserLookupHandler;
import io.ipolyzos.config.AppConfig;
import io.ipolyzos.models.EnrichedOrder;
import io.ipolyzos.models.Item;
import io.ipolyzos.models.Order;
import io.ipolyzos.models.OrderWithUserData;
import io.ipolyzos.models.User;
import io.ipolyzos.utils.EnvironmentUtils;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.connector.pulsar.source.PulsarSource;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.pulsar.client.api.SubscriptionType;

public class LookupStream {

    public static void main(String[] args) throws Exception {
        // 1. Initialize the execution environment
        StreamExecutionEnvironment env = EnvironmentUtils.initEnvWithWebUI(false);

        env.setParallelism(1);

        // Checkpoints configurations
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointStorage(AppConfig.checkpointDir);
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );

        // Configure Restart Strategy
        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(5, Time.of(10, TimeUnit.SECONDS))
        );


        // 2. Initialize Sources
        PulsarSource<User> userSource =
                EnvironmentUtils.initPulsarSource(
                        AppConfig.USERS_TOPIC,
                        "flink-user-consumer",
                        SubscriptionType.Exclusive,
                        StartCursor.earliest(),
                        User.class);

        PulsarSource<Item> itemSource =
                EnvironmentUtils.initPulsarSource(
                        AppConfig.ITEMS_TOPIC,
                        "flink-items-consumer",
                        SubscriptionType.Exclusive,
                        StartCursor.earliest(),
                        Item.class);

        PulsarSource<Order> orderSource =
                EnvironmentUtils.initPulsarSource(
                        AppConfig.ORDERS_TOPIC,
                        "flink-orders-consumer",
                        SubscriptionType.Exclusive,
                        StartCursor.latest(),
                        Order.class);

        WatermarkStrategy<Order> watermarkStrategy =
                WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(
                                (SerializableTimestampAssigner<Order>) (order, l) -> order.getCreatedAt()
                        );

        // 3. Initialize Streams
        DataStream<User> userStream =
                env.fromSource(userSource, WatermarkStrategy.noWatermarks(), "Pulsar User Source")
//                        .setParallelism(1)
                        .name("pulsarUserSource")
                        .uid("pulsarUserSource");

        DataStream<Item> itemStream =
                env.fromSource(itemSource, WatermarkStrategy.noWatermarks(), "Pulsar Items Source")
//                        .setParallelism(1)
                        .name("pulsarItemSource")
                        .uid("pulsarItemSource");

        DataStream<Order> orderStream = env.fromSource(orderSource, watermarkStrategy, "Pulsar Orders Source")
//                .setParallelism(1)
                .name("pulsarOrderSource")
                .uid("pulsarOrderSource");

        DataStream<EnrichedOrder> enrichedStream = orderStream
                .keyBy(Order::getUserId)
                .connect(userStream.keyBy(User::getId))
                .process(new UserLookupHandler())
                .uid("usersLookup")
                .name("usersLookup")
                .keyBy(OrderWithUserData::getItemId)
                .connect(itemStream.keyBy(Item::getId))
                .process(new ItemLookupHandler())
                .uid("itemsLookup")
                .name("itemsLookup");


        enrichedStream
                .print()
                .uid("print")
                .name("print");
        env.execute("Order Enrichment Stream");
    }
}
