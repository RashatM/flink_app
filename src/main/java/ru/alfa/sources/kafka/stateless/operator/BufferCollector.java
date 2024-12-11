package ru.alfa.sources.kafka.stateless.operator;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class BufferCollector {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);

        DataStream<String> inputStream = env.socketTextStream("localhost", 9999);

        DataStream<String> processedStream = inputStream.flatMap(new MyOperatorFunction());

        processedStream.print();

        env.execute("Operator State Example");

    }
}
