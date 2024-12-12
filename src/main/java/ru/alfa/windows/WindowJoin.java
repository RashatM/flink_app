package ru.alfa.windows;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;
import ru.alfa.windows.dto.EnrichedTrade;
import ru.alfa.windows.dto.events.Event;
import ru.alfa.windows.dto.events.TradeQuoteEvent;
import ru.alfa.windows.dto.events.TradeTransactionEvent;

import java.time.Duration;


public class WindowJoin {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<TradeTransactionEvent> tradeStream = env.fromData(
                new TradeTransactionEvent(2000L, "LKOH", 10, 100),
                new TradeTransactionEvent(5000L, "LKOH", 15, 120),
                new TradeTransactionEvent(9000L, "LKOH", -10, 130)
        ).assignTimestampsAndWatermarks(new CustomWatermarkStrategy<>());

        DataStream<TradeQuoteEvent> quoteStream = env.fromData(
                new TradeQuoteEvent(1000L, "LKOH", 110),
                new TradeQuoteEvent(3000L, "LKOH", 110),
                new TradeQuoteEvent(4000L, "LKOH", 115),
                new TradeQuoteEvent(12000L, "LKOH", 90)
        ).assignTimestampsAndWatermarks(WatermarkStrategy
                .<TradeQuoteEvent>forMonotonousTimestamps() // стратегия водяных знаков
                .withTimestampAssigner((event, timestamp) -> event.getTimestamp()) // устанавливаем timestamp из данных
        );

        DataStream<EnrichedTrade> enrichedTradeDataStream = tradeStream
                .keyBy(TradeTransactionEvent::getTrade)
                .intervalJoin(quoteStream.keyBy(TradeQuoteEvent::getTrade))
                .between(Duration.ofSeconds(-1), Duration.ofSeconds(1))
                .process(new ProcessJoinFunction<>() {
                    @Override
                    public void processElement(
                            TradeTransactionEvent trade,
                            TradeQuoteEvent quote,
                            Context context,
                            Collector<EnrichedTrade> collector
                    ) throws Exception {
                        double priceDifference = trade.getPrice() - quote.getPrice();
                        collector.collect(new EnrichedTrade(trade, quote, priceDifference));
                    }
                });

        enrichedTradeDataStream.print();
        env.execute();
    }
}
