package io.ipolyzos.compute.handlers;

import io.ipolyzos.models.EnrichedOrder;
import io.ipolyzos.models.Item;
import io.ipolyzos.models.OrderWithUserData;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ItemLookupHandler extends CoProcessFunction<OrderWithUserData, Item, EnrichedOrder> {
    private static final Logger logger = LoggerFactory.getLogger(UserLookupHandler.class);
    private ValueState<Item> itemState;

    @Override
    public void open(Configuration parameters) throws Exception {
        logger.info("{}, initializing state ...", this.getClass().getSimpleName());

        itemState = getRuntimeContext()
                .getState(
                        new ValueStateDescriptor<Item>("itemState", Item.class)
                );
    }

    @Override
    public void processElement1(OrderWithUserData order,
                                CoProcessFunction<OrderWithUserData, Item, EnrichedOrder>.Context context,
                                Collector<EnrichedOrder> collector) throws Exception {
        Item item = itemState.value();
        if (item == null) {
            logger.warn("Failed to find state for id '{}'", order.getItemId());
        } else {
            collector.collect(
                    new EnrichedOrder(
                            order.getInvoiceId(),
                            order.getLineItemId(),
                            order.getUser(),
                            item,
                            order.getCreatedAt(),
                            order.getPaidAt()
                    )
            );
        }
    }

    @Override
    public void processElement2(Item item,
                                CoProcessFunction<OrderWithUserData, Item, EnrichedOrder>.Context context,
                                Collector<EnrichedOrder> collector) throws Exception {
        itemState.update(item);
    }
}
