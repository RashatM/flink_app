package org.example;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Main {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        String path = "/Users/rmusin/IdeaProjects/flink_app/target/classes/wpop.csv";
//        CsvReaderFormat<CityRecord> csvFormat = CsvReaderFormat.forPojo(CityRecord.class);
//
//
//        // Чтение CSV данных
//        FileSource<CityRecord> source = FileSource
//                .forRecordStreamFormat(csvFormat, new Path(path))
//                .build();
//
//        DataStream<CityRecord> dataStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "csv-source");
//        dataStream
//                .map(new MapFunction<CityRecord, Tuple2<String, Integer>>() {
//                    @Override
//                    public Tuple2<String, Integer> map(CityRecord record) {
//                        return Tuple2.of(record.getCountry(), 1);
//                    }
//                })
//                .keyBy(0)
//                .sum(1)
//                .print();
        DataStream<String> dataStream = env.readTextFile(path);
//        dataStream
//                .map(new MapFunction<String, Tuple2<String, Integer>>() {
//                    @Override
//                    public Tuple2<String, Integer> map(String line) throws Exception {
//                        String[] fields = line.split(",");
//                        String country = fields[0]; // Извлекаем страну из первой колонки
//                        return Tuple2.of(country, 1); // Возвращаем пару
//                    }
//                })
//                .keyBy(0) // Группируем по стране
//                .sum(1) // Суммируем количество городов для каждой страны
//                .print(); // Печатаем результат

        DataStream<CountryCount> countryCounts = dataStream
                .map(new MapFunction<String, CountryCount>() {
                    @Override
                    public CountryCount map(String line) throws Exception {
                        String[] fields = line.split(",");
                        String country = fields[0]; // Извлекаем страну из первой колонки
                        return new CountryCount(country, 1); // Возвращаем объект
                    }
                })
                .keyBy(countryCount -> countryCount.country) // Группируем по стране
                .reduce((value1, value2) -> new CountryCount(value1.country, value1.count + value2.count)); // Суммируем количество
        env.execute();
    }
}