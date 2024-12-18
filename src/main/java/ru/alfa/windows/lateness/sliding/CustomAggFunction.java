package ru.alfa.windows.lateness.sliding;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.checkerframework.checker.units.qual.A;

public class CustomAggFunction implements AggregateFunction<Tuple2<Long, Integer>, AggResult, AggResult> {
    @Override
    public AggResult createAccumulator() {
        return new AggResult(0L, 0L, 0.0, 0L);
    }

    @Override
    public AggResult add(Tuple2<Long, Integer> value, AggResult accumulator) {
        long sum = accumulator.getSum() + value.f0;
        long max = Math.max(accumulator.getMax(), value.f0);
        long count = accumulator.getCount() + 1;
        double average = accumulator.getCount() > 0 ? (double) accumulator.getSum() / accumulator.getCount() : 0;

        return new AggResult(sum, max, average, count);
    }

    @Override
    public AggResult getResult(AggResult accumulator) {
        return accumulator;
    }

    @Override
    public AggResult merge(AggResult a, AggResult b) {
        return new AggResult(
                a.getSum() + b.getSum(),
                a.getMax() + b.getMax(),
                a.getAvg() + b.getAvg(),
                a.getCount() + b.getCount()
        );
    }

}
