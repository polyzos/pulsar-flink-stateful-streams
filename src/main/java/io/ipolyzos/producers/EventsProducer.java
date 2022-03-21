package io.ipolyzos.producers;

import io.ipolyzos.config.AppConfig;
import io.ipolyzos.models.Event;
import io.ipolyzos.utils.ClientUtils;
import io.ipolyzos.utils.DataSourceUtils;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventsProducer {
    private static final Logger logger
            = LoggerFactory.getLogger(EventsProducer.class);

    private static HashMap<String, CompletableFuture<Producer<Event>>> producerCache = new HashMap<>();

    public static void main(String[] args) throws IOException {
        Stream<Event> events = DataSourceUtils.loadDataFile(AppConfig.EVENTS_FILE_PATH)
                .map(DataSourceUtils::lineAsEvent);

        logger.info("Creating Pulsar Client ...");
        PulsarClient pulsarClient = ClientUtils.initPulsarClient(AppConfig.token);

        AtomicInteger viewEventsCounter = new AtomicInteger();
        AtomicInteger cartEventsCounter = new AtomicInteger();
        AtomicInteger purchaseEventsCounter = new AtomicInteger();

        ExecutorService executorService = Executors.newFixedThreadPool(3);

        for (Iterator<Event> it = events.iterator(); it.hasNext(); ) {
            Event event = it.next();
            if (event.getEventType().equals("view")) {
                getProducer(pulsarClient, AppConfig.VIEW_EVENTS_TOPIC)
                        .thenAcceptAsync(producer -> {
                            produceMessage(producer, event.getUserid(), event, viewEventsCounter);
                        }, executorService);
            } else if (event.getEventType().equals("cart")) {
                getProducer(pulsarClient, AppConfig.CART_EVENTS_TOPIC)
                        .thenAcceptAsync(producer -> {
                            produceMessage(producer, event.getUserid(), event, cartEventsCounter);
                        }, executorService);
            } else if (event.getEventType().equals("purchase")) {
                getProducer(pulsarClient, AppConfig.PURCHASE_EVENTS_TOPIC)
                        .thenAcceptAsync(producer -> {
                            produceMessage(producer, event.getUserid(), event, purchaseEventsCounter);
                        }, executorService);
            }
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Sent >> {} 'VIEW' events  >> {} 'PURCHASE' events and >> {} 'CART' events.", viewEventsCounter.get(), purchaseEventsCounter.get(), cartEventsCounter.get());
            logger.info("Closing Resources...");
            try {
                producerCache.values().forEach(cf -> {
                    try {
                        cf.get().close();
                    } catch (PulsarClientException | InterruptedException | ExecutionException e) {
                        e.printStackTrace();
                    }
                });
                pulsarClient.close();
            } catch (PulsarClientException e) {
                e.printStackTrace();
            }
        }));
    }

    private static void produceMessage(Producer<Event> producer, String key, Event value, AtomicInteger counter) {
        logger.info("[{}] Sending to message: {}", producer.getProducerName(), value);
        producer.newMessage()
                .key(key)
                .value(value)
                .eventTime(System.currentTimeMillis())
                .sendAsync()
                .whenComplete((id, exception) -> {
                    if (exception != null) {
                        logger.error("❌ Failed message: {}", exception.getMessage());
                    } else {
                        logger.info("✅ Acked message {} - Total {}", id, counter.getAndIncrement());
                    }
                });
    }

    private static CompletableFuture<Producer<Event>> getProducer(PulsarClient pulsarClient, String topicName) {
        return producerCache.computeIfAbsent(topicName, (tn) -> {
            logger.info("Creating Producer for topic '{}' ...", tn);
            return pulsarClient
                    .newProducer(JSONSchema.of(Event.class))
                    .producerName("producer-" + topicName)
                    .topic(tn)
                    .blockIfQueueFull(true)
                    .createAsync();
        });
    }
}
