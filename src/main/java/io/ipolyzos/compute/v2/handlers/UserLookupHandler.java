package io.ipolyzos.compute.v2.handlers;

import io.ipolyzos.models.Order;
import io.ipolyzos.models.OrderWithUserData;
import io.ipolyzos.models.User;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UserLookupHandler extends CoProcessFunction<Order, User, OrderWithUserData> {
    private static final Logger logger = LoggerFactory.getLogger(UserLookupHandler.class);
//    private final OutputTag<EnrichedOrder> missingStateTag;
    private ValueState<User> userState;

//    public UserLookupHandler(OutputTag<EnrichedOrder> missingStateTag) {
//        this.missingStateTag = missingStateTag;
//    }

    @Override
    public void open(Configuration parameters) throws Exception {
        logger.info("{}, initializing state ...", this.getClass().getSimpleName());

        userState = getRuntimeContext()
                .getState(
                        new ValueStateDescriptor<User>("userState", User.class)
                );
    }

    @Override
    public void processElement1(Order order, CoProcessFunction<Order, User, OrderWithUserData>.Context context,
                                Collector<OrderWithUserData> collector) throws Exception {
        User user = userState.value();
        if (user == null) {
            logger.warn("Failed to find state for id '{}'", order.getUserId());
//            EnrichedOrder enrichedOrder =
//                    new EnrichedOrder(order.getInvoiceId(), order.getLineItemId(), null, null, order.getCreatedAt(),
//                            order.getPaidAt());
//            context.output(missingStateTag, enrichedOrder);
        } else {
            collector.collect(order.withUserData(user));
        }
    }

    @Override
    public void processElement2(User user,
                                CoProcessFunction<Order, User, OrderWithUserData>.Context context,
                                Collector<OrderWithUserData> collector) throws Exception {
        userState.update(user);
    }
}
