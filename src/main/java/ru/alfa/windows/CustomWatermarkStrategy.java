package ru.alfa.windows;

import org.apache.flink.api.common.eventtime.*;
import ru.alfa.windows.dto.events.Event;

import java.time.Duration;

public class CustomWatermarkStrategy<T extends Event> implements WatermarkStrategy<T> {

    private final Duration maxOutOfOrderness;

    public CustomWatermarkStrategy(Duration maxOutOfOrderness) {
        this.maxOutOfOrderness = maxOutOfOrderness;
    }

    @Override
    public WatermarkGenerator<T> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new CustomWatermarkGenerator<>(maxOutOfOrderness);
    }

    @Override
    public TimestampAssigner<T> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return new CustomTimestampAssigner<>();
    }
}
