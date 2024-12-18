package ru.alfa.windows.dto.events;

import lombok.ToString;

@ToString
public class TradeTransactionEvent implements Event {
    private long timestamp; // Время сделки
    private String trade;      // Название акции (например, "LKOH", "GAZP", "SNGZ")
    private int quantity;      // Количество акций (положительное значение для покупки, отрицательное для продажи)
    private  int price;

    public TradeTransactionEvent(long timestamp, String trade, int quantity, int price) {
        this.timestamp = timestamp;
        this.trade = trade;
        this.quantity = quantity;
        this.price = price;
    }

    public TradeTransactionEvent() {};

    public long getTimestamp() {
        return timestamp;
    }

    public String getTrade() {
        return trade;
    }

    public int getQuantity() {
        return quantity;
    }

    public int getPrice() {
        return price;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }


    public void setTrade(String trade) {
        this.trade = trade;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    public void setPrice(int price) {
        this.price = price;
    }
}

