package io.ipolyzos.producers;

import io.ipolyzos.config.AppConfig;
import io.ipolyzos.models.Item;
import io.ipolyzos.models.User;
import io.ipolyzos.utils.ClientUtils;
import java.io.IOException;
import java.util.Iterator;
import io.ipolyzos.utils.DataSourceUtils;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.stream.Stream;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnrichmentDataSource {
    private static final Logger logger
            = LoggerFactory.getLogger(EnrichmentDataSource.class);

    public static void main(String[] args) throws IOException {
        Stream<User> userStream = DataSourceUtils.loadDataFile(AppConfig.USERS_FILE_PATH)
                .map(DataSourceUtils::lineAsUser);

        Stream<Item> itemsStream = DataSourceUtils.loadDataFile(AppConfig.ITEMS_FILE_PATH)
                .map(DataSourceUtils::lineAsItem);

        logger.info("Creating Pulsar Client ...");
        PulsarClient pulsarClient = ClientUtils.initPulsarClient(AppConfig.token);

        logger.info("Creating User Producer ...");
        Producer<User> userProducer
                = createProducer(pulsarClient, "user-producer", AppConfig.USERS_TOPIC, User.class);

        AtomicInteger userCounter = new AtomicInteger();
        for (Iterator<User> it = userStream.iterator(); it.hasNext(); ) {
            User user = it.next();

            produceMessage(userProducer, String.valueOf(user.getId()), user, userCounter);
        }

        Producer<Item> itemProducer
                = createProducer(pulsarClient, "item-producer", AppConfig.ITEMS_TOPIC, Item.class);

        AtomicInteger itemsCounter = new AtomicInteger();
        for (Iterator<Item> it = itemsStream.iterator(); it.hasNext(); ) {
            Item item = it.next();

            produceMessage(itemProducer, String.valueOf(item.getId()), item, itemsCounter);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Sent '{}' user records and '{}' item records.", userCounter.get(), itemsCounter.get());
            logger.info("Closing Resources...");
            try {
                userProducer.close();
                itemProducer.close();
                pulsarClient.close();
            } catch (PulsarClientException e) {
                e.printStackTrace();
            }
        }));
    }

    private static <T> Producer<T> createProducer(PulsarClient pulsarClient,
                                                  String producerName,
                                                  String topicName,
                                                  Class<T> classz) throws PulsarClientException {
        logger.info("Creating {} Producer ...", classz.getSimpleName());
        return pulsarClient.newProducer(JSONSchema.of(classz))
                .producerName(producerName)
                .topic(topicName)
                .blockIfQueueFull(true)
                .create();
    }

    private static <T> void produceMessage(Producer<T> producer, String key, T value, AtomicInteger counter) {
            producer.newMessage()
                    .key(key)
                    .value(value)
                    .eventTime(System.currentTimeMillis())
                    .sendAsync()
                    .whenComplete(callback(counter));
        }

    private static BiConsumer<MessageId, Throwable> callback(AtomicInteger counter) {
        return (id, exception) -> {
            if (exception != null) {
                logger.error("❌ Failed message: {}", exception.getMessage());
            } else {
                logger.info("✅ Acked message {} - Total {}", id, counter.getAndIncrement());
            }
        };
    }
}
