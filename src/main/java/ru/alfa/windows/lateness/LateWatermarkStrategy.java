package ru.alfa.windows.lateness;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.java.tuple.Tuple2;

import java.time.Duration;

public class LateWatermarkStrategy implements WatermarkStrategy<Tuple2<Long, Integer>> {

    private final Duration maxOutOfOrderness;


    public LateWatermarkStrategy(Duration maxOutOfOrderness) {
        this.maxOutOfOrderness = maxOutOfOrderness;
    }

    public LateWatermarkStrategy() {
        this.maxOutOfOrderness = Duration.ofSeconds(0);
    }


    @Override
    public WatermarkGenerator<Tuple2<Long, Integer>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new LateWatermarkGenerator(maxOutOfOrderness);
    }

    @Override
    public TimestampAssigner<Tuple2<Long, Integer>> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return new LateTimestampAssigner();
    }
}
