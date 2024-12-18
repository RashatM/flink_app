package ru.alfa.windows.joins;

import org.apache.flink.api.common.eventtime.TimestampAssigner;
import ru.alfa.windows.dto.events.Event;


public class CustomTimestampAssigner<T extends Event> implements TimestampAssigner<T> {

    @Override
    public long extractTimestamp(T event, long recordTimestamp) {
        return event.getTimestamp();
    }
}