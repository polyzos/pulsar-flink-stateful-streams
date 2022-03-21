package io.ipolyzos.config;

import java.util.Optional;

public class AppConfig {
    public static final String SERVICE_HTTP_URL = "http://localhost:8080";
    public static final String SERVICE_URL      = "pulsar://localhost:6650";

    public static final String ORDERS_TOPIC = "orders";
    public static final String USERS_TOPIC  = "users";
    public static final String ITEMS_TOPIC  = "items";

    // Input File Sources
    public static final String ORDERS_FILE_PATH = "/data/orders.csv";
    public static final String USERS_FILE_PATH  = "/data/users.csv";
    public static final String ITEMS_FILE_PATH  = "/data/items.csv";

    public static final Optional<String> token = Optional.empty();

    public static final String checkpointDir =  "file:///" + System.getProperty("user.dir") +"/checkpoints/";

}
