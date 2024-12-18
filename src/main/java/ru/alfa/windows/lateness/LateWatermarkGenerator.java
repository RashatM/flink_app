package ru.alfa.windows.lateness;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.java.tuple.Tuple2;

import java.time.Duration;

public class LateWatermarkGenerator implements WatermarkGenerator<Tuple2<Long, Integer>> {
    private final long maxOutOfOrdernessMillis;
    private long currentMaxTimestamp;

    public LateWatermarkGenerator(Duration maxOutOfOrderness) {
        this.maxOutOfOrdernessMillis = maxOutOfOrderness.toMillis();
        this.currentMaxTimestamp = Long.MIN_VALUE;
    }

    @Override
    public void onEvent(Tuple2<Long, Integer> event, long eventTimestamp, WatermarkOutput watermarkOutput) {
        currentMaxTimestamp = Math.max(currentMaxTimestamp, eventTimestamp);
        System.out.println("currentMaxTimestamp: "+ currentMaxTimestamp + " " + (currentMaxTimestamp - maxOutOfOrdernessMillis));
        watermarkOutput.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrdernessMillis));
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
        watermarkOutput.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrdernessMillis));
    }
}
