package ru.alfa.windows.dto;

import lombok.ToString;
import ru.alfa.windows.dto.events.TradeQuoteEvent;
import ru.alfa.windows.dto.events.TradeTransactionEvent;

@ToString
public class EnrichedTrade {
    private TradeTransactionEvent trade;
    private TradeQuoteEvent quote;
    private double priceDifference;

    public EnrichedTrade(TradeTransactionEvent trade, TradeQuoteEvent quote, double priceDifference) {
        this.trade = trade;
        this.quote = quote;
        this.priceDifference = priceDifference;
    }

    public TradeTransactionEvent getTrade() {
        return trade;
    }

    public TradeQuoteEvent getQuote() {
        return quote;
    }

    public double getPriceDifference() {
        return priceDifference;
    }
}



