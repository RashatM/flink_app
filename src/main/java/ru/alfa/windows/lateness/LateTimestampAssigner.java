package ru.alfa.windows.lateness;

import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.java.tuple.Tuple2;


public class LateTimestampAssigner implements TimestampAssigner<Tuple2<Long, Integer>> {

    @Override
    public long extractTimestamp(Tuple2<Long, Integer> event, long recordTimestamp) {
        return event.f0;
    }
}