package ru.alfa.sources.kafka.stateless;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import ru.alfa.sources.kafka.stateless.schemas.InsuranceDataDeserializationSchema;
import ru.alfa.sources.kafka.stateless.schemas.InsuranceDataSerializationSchema;


public class RequisitionProcessor {
    private static final String BOOTSTRAP_SERVERS = "kafka:9092";
    private static final String CONSUMER_GROUP = "my-group";
    private static final String SOURCE_TOPIC = "requests";
    private static final String SINK_TOPIC = "lab03_09_doubles";

    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);

        KafkaSource<InsuranceData> source = KafkaSource.<InsuranceData>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics(SOURCE_TOPIC)
                .setGroupId(CONSUMER_GROUP)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new InsuranceDataDeserializationSchema())
                .build();

        KafkaSink<InsuranceData> sink = KafkaSink.<InsuranceData>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(SINK_TOPIC)
                        .setValueSerializationSchema(new InsuranceDataSerializationSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .keyBy((InsuranceData data) -> data.insurer)
                .flatMap(new KafkaValueState())
                .sinkTo(sink);
    }
}
