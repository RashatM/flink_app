package ru.alfa.sources.file.operators;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WithMap {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        String path = "/Users/rmusin/IdeaProjects/flink_app/target/classes/wpop.csv";

        DataStream<String> dataStream = env.readTextFile(path);
        dataStream
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String line) throws Exception {
                        String[] fields = line.split(",");
                        if (fields.length >= 0) {
                            String country = fields[0]; // Извлекаем страну из первой колонки
                            return Tuple2.of(country, 1); // Возвращаем пару
                        }
                        return null;
                    }
                })
                .filter(value -> value != null)
                .keyBy(0) // Группируем по стране
                .print(); // Печатаем результат


        env.execute();
    }
}