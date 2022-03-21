package io.ipolyzos.utils;

import io.ipolyzos.config.AppConfig;
import io.ipolyzos.models.Event;
import io.ipolyzos.models.Item;
import io.ipolyzos.models.Order;
import io.ipolyzos.models.User;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DataSourceUtils {
    public static void main(String[] args) throws IOException {
        List<Event> events = loadDataFile(AppConfig.EVENTS_FILE_PATH)
                .map(DataSourceUtils::lineAsEvent)
                .collect(Collectors.toList());

        Timestamp prev = new Timestamp(events.get(0).getEventTime());
        int outOfOrder = 0;
        List<Timestamp> timestamps = events.stream()
                .map(e -> {
                    return new Timestamp(e.getEventTime());
                }).collect(Collectors.toList());

        for (Timestamp current : timestamps) {
            if (current.before(prev)) {
                System.out.println("Out of order: " + prev + " - " + current);
                outOfOrder += 1;
            }
            prev = current;
        }

        System.out.println(timestamps.size());
        System.out.println("Total out of order events: " + outOfOrder);

        System.out.println("Total: " + events.size());
        List<String> types
                = events.stream()
                .map(Event::getEventType)
                .distinct()
                .collect(Collectors.toList());
        System.out.println("Types:");
        types.forEach(t -> System.out.println("\t" + t));

        long views
                = events.stream().filter(e -> e.getEventType().equals("view")).count();
        long carts = events.stream().filter(e -> e.getEventType().equals("cart")).count();
        long purchases = events.stream().filter(e -> e.getEventType().equals("purchase")).count();

        System.out.println("View: " + views);
        System.out.println("Carts: " + carts);
        System.out.println("Purchases: " + purchases);

        long count = events.stream().map(Event::getUserid).distinct().count();
        System.out.println("User ids: " + count);
        System.out.println("file:///" + System.getProperty("user.dir") +"/checkpoints/");
    }

    public static Stream<String> loadDataFile(String fileName) throws IOException {
        return Files.lines(
                Paths.get(System.getProperty("user.dir") +  fileName)
        ).skip(1);
    }

    public static Item lineAsItem(String line) {
        String[] tokens = line.split(",");
        long id = Integer.parseInt(tokens[0]);
        long createdAtEpoch = Long.parseLong(tokens[2]);
        double price
                = Double.parseDouble(tokens[7]);
        return new Item(id, createdAtEpoch, tokens[3], tokens[4], tokens[5], tokens[6], price);
    }

    public static Order lineAsOrder(String line) {
        String[] tokens = line.split(",");
        long invoiceId = parseId(tokens[0]);
        long lineItemId = parseId(tokens[1]);
        long userId = parseId(tokens[2]);
        long itemId = parseId(tokens[3]);

        long createdAt = Timestamp.valueOf(tokens[7]).getTime();
        long paidAt = Timestamp.valueOf(tokens[8]).getTime();
        double price
                = Double.parseDouble(tokens[6]);
        return new Order(invoiceId, lineItemId, userId, itemId, tokens[4], tokens[5], price, createdAt, paidAt);
    }

    public static User lineAsUser(String line) {
        String[] tokens = line.split(",", -1);
        long id = parseId(tokens[0]);
        long createdAt = Long.parseLong(tokens[4]);

        long deletedAt = -1;
        if (!tokens[5].isEmpty()) {
            deletedAt = Long.parseLong(tokens[5]);
        }

        long mergedAt = -1;
        if (!tokens[6].trim().isEmpty()) {
            mergedAt = Long.parseLong(tokens[6]);
        }
        long parentUserId = -1;
        if (!tokens[7].isEmpty()) {
            parentUserId = parseId(tokens[7]);
        }

        return new User(id, tokens[2], tokens[3], tokens[3], createdAt, deletedAt, mergedAt, parentUserId);
    }


    public static Event lineAsEvent(String line) {
        String[] tokens = line.split(",");
        Timestamp timestamp = Timestamp.valueOf(tokens[0]);
        return new Event(timestamp.getTime(), tokens[1], tokens[2], tokens[3], tokens[4], tokens[5], Double.parseDouble(tokens[6]), tokens[7], tokens[8]);
    }

    private static long parseId(String id) {
        return (long) Double.parseDouble(id.replace(".0", ""));
    }
}
