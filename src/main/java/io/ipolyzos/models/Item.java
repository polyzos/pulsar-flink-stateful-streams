package io.ipolyzos.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Item {
    private long id;
    private long createdAt;
    private String adjective;
    private String category;
    private String modifier;
    private String name;
    private double price;
}
