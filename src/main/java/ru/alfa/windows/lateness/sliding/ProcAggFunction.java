package ru.alfa.windows.lateness.sliding;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class ProcAggFunction extends ProcessWindowFunction<Tuple2<Long, Integer>, String, Integer, TimeWindow>{
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
                "; Count: " + count +
                "; Watermark: " + ctx.currentWatermark()
        );
    }
}
