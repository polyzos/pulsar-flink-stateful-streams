package io.ipolyzos.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Order {
    private long invoiceId;
    private long lineItemId;
    private long userId;
    private long itemId;
    private String itemName;
    private String itemCategory;
    private double price;
    private long createdAt;
    private long paidAt;

    public OrderWithUserData withUserData(User user) {
        return new OrderWithUserData(
                this.invoiceId,
                this.lineItemId,
                user,
                this.itemId,
                this.itemName,
                this.itemCategory,
                this.price,
                this.createdAt,
                this.paidAt
        );
    }
}
