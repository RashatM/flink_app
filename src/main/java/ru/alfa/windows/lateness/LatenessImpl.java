package ru.alfa.windows.lateness;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class LatenessImpl {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Создание побочного выхода для опоздавших данных
        OutputTag<Tuple2<Long, Integer>> lateOutputTag = new OutputTag<>("late-data") {};

        // Основной поток данных
        DataStream<Tuple2<Long, Integer>> dataStream = env.fromData(
                new Tuple2<>(500L, 1),
                new Tuple2<>(1500L, 1),
                new Tuple2<>(2500L, 1),
                new Tuple2<>(3100L, 1),
                new Tuple2<>(3500L, 1),
                new Tuple2<>(2300L, 1),
                new Tuple2<>(2400L, 1)
        );

        SingleOutputStreamOperator<String> mainStream = dataStream
                .assignTimestampsAndWatermarks(new LateWatermarkStrategy(Duration.ofMillis(100)))
                .keyBy(t -> t.f1)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(3)))
                .allowedLateness(Duration.ofMillis(500))
                .sideOutputLateData(lateOutputTag)
                .process(new ProcessWindowFunction<Tuple2<Long, Integer>, String, Integer, TimeWindow>() {
                    @Override
                    public void process(
                            Integer key,
                            Context ctx,
                            Iterable<Tuple2<Long, Integer>> elements,
                            Collector<String> out)  {
                        System.out.println("currentWatermark=" + ctx.currentWatermark());
                        out.collect("Window: " + ctx.window().getStart() + "-" + ctx.window().getEnd() +
                                "; Key: " + key +
                                "; Values: " + elements);
                    }
                });

        mainStream.print();

//        mainStream.getSideOutput(lateOutputTag).print();
        env.execute();
    }
}
