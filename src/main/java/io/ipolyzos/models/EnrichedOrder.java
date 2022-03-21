package io.ipolyzos.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class EnrichedOrder {
    private long invoiceId;
    private long lineItemId;
    private User user;
    private Item item;
    private long createdAt;
    private long paidAt;
}
