package com.hacksys.backend.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class InventoryItem {

    private String productId;
    private String name;
    // Intentional: AtomicInteger gives atomic single-ops but NOT atomic check-then-act
    private final AtomicInteger stock = new AtomicInteger(0);
    // FIX INC-20260513083526-6D2938: back reservedStock with AtomicInteger to prevent
    // lost-update races when multiple threads increment the counter concurrently.
    private final AtomicInteger reservedStockRef = new AtomicInteger(0);
    private double price;
    private Instant lastUpdated;
    // Intentional: no update history / audit trail
    private String lastUpdatedBy;

    public InventoryItem() {}

    public InventoryItem(String productId, String name, int initialStock, double price) {
        this.productId = productId;
        this.name = name;
        this.stock.set(initialStock);
        this.price = price;
        this.reservedStockRef.set(0);
        this.lastUpdated = Instant.now();
    }

    public String getProductId() { return productId; }
    public void setProductId(String productId) { this.productId = productId; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public int getStock() { return stock.get(); }
    public void setStock(int value) {
        stock.set(value);
        this.lastUpdated = Instant.now();
    }

    public AtomicInteger getStockRef() { return stock; }

    // FIX INC-20260513083526-6D2938: delegate to AtomicInteger so all callers get
    // a consistent, thread-safe view of the reserved quantity.
    public int getReservedStock() { return reservedStockRef.get(); }
    public void setReservedStock(int value) { reservedStockRef.set(value); }
    public AtomicInteger getReservedStockRef() { return reservedStockRef; }

    public double getPrice() { return price; }
    public void setPrice(double price) { this.price = price; }

    public Instant getLastUpdated() { return lastUpdated; }
    public void setLastUpdated(Instant lastUpdated) { this.lastUpdated = lastUpdated; }

    public String getLastUpdatedBy() { return lastUpdatedBy; }
    public void setLastUpdatedBy(String lastUpdatedBy) { this.lastUpdatedBy = lastUpdatedBy; }
}
