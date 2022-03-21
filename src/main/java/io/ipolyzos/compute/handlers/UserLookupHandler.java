package io.ipolyzos.compute.handlers;

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
    private ValueState<User> userState;

    private int totalPresentState = 0;
    private int totalMissingState = 0;
    @Override
    public void open(Configuration parameters) throws Exception {
        logger.info("LookupStreamHandler, initializing state ...");

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
        } else {
            collector.collect(order.withUserData(user));
        }
    }

    @Override
    public void processElement2(User user,
                                CoProcessFunction<Order, User, OrderWithUserData>.Context context,
                                Collector<OrderWithUserData> collector) throws Exception {
        User value = userState.value();
        if (value == null) {
            logger.warn("No state for key '{}'.", user.getId());
            totalMissingState += 1;
        } else {
            logger.info("State already present for key '{}'.", user.getId());
            totalPresentState += 1;
        }
        logger.info("Total - present state '{}', missing state '{}'", totalPresentState, totalMissingState);
        userState.update(user);
    }
}
