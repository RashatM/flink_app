package ru.alfa.windows.lateness.sliding;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.util.OutputTag;
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
                new Tuple2<>(2100L, 1),
                new Tuple2<>(3600L, 1),
                new Tuple2<>(2300L, 1),
                new Tuple2<>(4200L, 1),
                new Tuple2<>(1000L, 1) // Опоздавшее событие
        );

        SingleOutputStreamOperator<String> mainStream = dataStream
                .assignTimestampsAndWatermarks(new LateWatermarkStrategy(Duration.ofMillis(0)))
                .keyBy(t -> t.f1)
                .window(SlidingEventTimeWindows.of(Duration.ofSeconds(3), Duration.ofSeconds(1)))
                .sideOutputLateData(lateOutputTag)
//                .aggregate(new CustomAggFunction());
                .process(new ProcWindowAggFunction());

        DataStream<Tuple2<Long, Integer>> lateStream = mainStream.getSideOutput(lateOutputTag);
        mainStream.print();
        lateStream.print();
        env.execute();
    }
}
