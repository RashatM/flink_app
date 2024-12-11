package ru.alfa.sources.kafka.stateless;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class RequisitionProcessor {

    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("kafka:9092")
                .setTopics("requests")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> dataStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        dataStream = dataStream
                .map((MapFunction<String, InsuranceData>) line -> {
                    String[] fields = line.split(";");

                    String insurer = fields[0].split("=")[1];
                    String model = fields[1].split("=")[1];
                    String insuranceType = fields[2];
                    return new InsuranceData(insurer, model, insuranceType);
                })
                .keyBy((InsuranceData data) -> data.insurer)
                .flatMap(new KafkaValueState())
                .map((MapFunction<InsuranceData, String>) insuranceData -> {
                    // Преобразуем объект InsuranceData обратно в строку для отправки в Kafka
                    return "insurer=" + insuranceData.getInsurer() + ";"
                            + "model=" + insuranceData.getModel() + ";"
                            + "insuranceType=" + insuranceData.getInsuranceType();
                });

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("kafka:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("lab03_NN_doubles")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
        
        dataStream.sinkTo(sink);
    }
}
