package io.ipolyzos.compute.v3;

import io.ipolyzos.compute.v3.handlers.ItemLookupHandler;
import io.ipolyzos.compute.v3.handlers.UserLookupHandler;
import io.ipolyzos.config.AppConfig;
import io.ipolyzos.models.EnrichedOrder;
import io.ipolyzos.models.Item;
import io.ipolyzos.models.Order;
import io.ipolyzos.models.OrderWithUserData;
import io.ipolyzos.models.User;
import io.ipolyzos.utils.EnvironmentUtils;
import java.time.Duration;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.pulsar.source.PulsarSource;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;
import org.apache.pulsar.client.api.SubscriptionType;

public class EnrichmentStream {

    public static void main(String[] args) throws Exception {
        // 1. Initialize the execution environment
        StreamExecutionEnvironment env = EnvironmentUtils.initEnvWithWebUI(false);


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
                        StartCursor.earliest(),
                        Order.class);

        WatermarkStrategy<Order> watermarkStrategy =
                WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(
                                (SerializableTimestampAssigner<Order>) (order, l) -> order.getCreatedAt()
                        );

        final OutputTag<EnrichedOrder> missingStateTagUsers = new OutputTag<>("missingState#User"){};
        final OutputTag<EnrichedOrder> missingStateTagItems = new OutputTag<>("missingState#Item"){};

        // 3. Initialize Streams
        DataStream<User> userStream =
                env.fromSource(userSource, WatermarkStrategy.noWatermarks(), "Pulsar User Source")
                        .name("pulsarUserSource")
                        .uid("pulsarUserSource");

        DataStream<Item> itemStream =
                env.fromSource(itemSource, WatermarkStrategy.noWatermarks(), "Pulsar Items Source")
                        .name("pulsarItemSource")
                        .uid("pulsarItemSource");

        DataStream<Order> orderStream = env.fromSource(orderSource, watermarkStrategy, "Pulsar Orders Source")
                .name("pulsarOrderSource")
                .uid("pulsarOrderSource");

        SingleOutputStreamOperator<OrderWithUserData> orderWithUserDataStream = orderStream
                .keyBy(Order::getUserId)
                .connect(userStream.keyBy(User::getId))
                .process(new UserLookupHandler(missingStateTagUsers))
                .uid("usersLookup")
                .name("usersLookup");

        SingleOutputStreamOperator<EnrichedOrder> enrichedOrderStream = orderWithUserDataStream
                .keyBy(OrderWithUserData::getItemId)
                .connect(itemStream.keyBy(Item::getId))
                .process(new ItemLookupHandler(missingStateTagItems))
                .uid("itemsLookup")
                .name("itemsLookup");

        enrichedOrderStream.getSideOutput(missingStateTagUsers)
                .printToErr()
                .name("MissingUserStateSink")
                .uid("MissingUserStateSink");

        enrichedOrderStream.getSideOutput(missingStateTagItems)
                .printToErr()
                .name("MissingItemStateSink")
                .uid("MissingItemStateSink");

        orderWithUserDataStream.getSideOutput(missingStateTagItems)
                        .print();
//        enrichedOrderStream
//                .print()
//                .uid("print")
//                .name("print");
        env.execute("Order Enrichment Stream");
    }
}
