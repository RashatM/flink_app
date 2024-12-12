package ru.alfa.windows.dto.events;


import lombok.ToString;

@ToString
public class TradeQuoteEvent implements Event {
    private Long timestamp;
    private String trade;      // Название акции (например, "LKOH", "GAZP", "SNGZ")
    private int price;

    public TradeQuoteEvent(long timestamp, String stock, int price) {
        this.timestamp = timestamp;
        this.trade = stock;
        this.price = price;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public void setPrice(int price) {
        this.price = price;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getTrade() {
        return trade;
    }

    public int getPrice() {
        return price;
    }

}
