package io.ipolyzos.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderWithUserData {
    private long invoiceId;
    private long lineItemId;
    private User user;
    private long itemId;
    private String itemName;
    private String itemCategory;
    private double price;
    private long createdAt;
    private long paidAt;
}
