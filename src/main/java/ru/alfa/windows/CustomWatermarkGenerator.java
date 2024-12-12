package ru.alfa.windows;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import ru.alfa.windows.dto.events.Event;

import java.time.Duration;

public class CustomWatermarkGenerator<T extends Event> implements WatermarkGenerator<T> {
    private final long maxOutOfOrdernessMillis;
    private long currentMaxTimestamp;

    public CustomWatermarkGenerator(Duration maxOutOfOrderness) {
        this.maxOutOfOrdernessMillis = maxOutOfOrderness.toMillis();
        this.currentMaxTimestamp = Long.MIN_VALUE;
    }

    @Override
    public void onEvent(T event, long eventTimestamp, WatermarkOutput watermarkOutput) {
        currentMaxTimestamp = Math.max(currentMaxTimestamp, eventTimestamp);
        watermarkOutput.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrdernessMillis));
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
        watermarkOutput.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrdernessMillis));
    }
}
