package ru.alfa.windows.lateness.sliding;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.checkerframework.checker.units.qual.A;
import ru.alfa.windows.lateness.LateWatermarkStrategy;

import java.time.Duration;

public class LatenessSlidingImpl {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Создание побочного выхода для опоздавших данных
        OutputTag<Tuple2<Long, Integer>> lateOutputTag = new OutputTag<>("late-data") {
        };

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
                .window(SlidingEventTimeWindows.of(Duration.ofSeconds(3), Duration.ofSeconds(1)))
                .sideOutputLateData(lateOutputTag)
//                .aggregate(new CustomAggFunction());
                .process(new ProcessWindowFunction<Tuple2<Long, Integer>, String, Integer, TimeWindow>() {
                    @Override
                    public void process(
                            Integer key,
                            Context ctx,
                            Iterable<Tuple2<Long, Integer>> elements,
                            Collector<String> out) throws Exception {

                        long sum = 0;
                        long max = Long.MIN_VALUE;
                        long count = 0;

                        // Проходим по элементам окна, вычисляем сумму, максимум и количество
                        for (Tuple2<Long, Integer> element : elements) {
                            sum += element.f0;
                            max = Math.max(max, element.f0);
                            count++;
                        }

                        double average = count > 0 ? (double) sum / count : 0;

                        out.collect("Window: " + ctx.window().getStart() + " to " + ctx.window().getEnd() +
                                "; Key: " + key +
                                "; Values: " + elements +
                                "; Average: " + average +
                                "; Max: " + max +
                                "; Sum: " + sum +
                                "; Count: " + count);
                    }
                });

        DataStream<Tuple2<Long, Integer>> lateStream = mainStream.getSideOutput(lateOutputTag);
        mainStream.print();
        lateStream.print();
        env.execute();
    }
}
