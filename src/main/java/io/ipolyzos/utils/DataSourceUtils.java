package io.ipolyzos.utils;

import io.ipolyzos.config.AppConfig;
import io.ipolyzos.models.Item;
import io.ipolyzos.models.Order;
import io.ipolyzos.models.User;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DataSourceUtils {
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

    private static long parseId(String id) {
        return (long) Double.parseDouble(id.replace(".0", ""));
    }
}
