package ru.alfa.sources.file;


import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

//не работает корректно, так как нет шапки и он сортирует атрибуты класса в алфавитном порядке
public class WithFileSource {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        String path = "/Users/rmusin/IdeaProjects/flink_app/target/classes/wpop.csv";
        CsvReaderFormat<CityRecord> csvFormat = CsvReaderFormat.forPojo(CityRecord.class);


        // Чтение CSV данных
        FileSource<CityRecord> source = FileSource
                .forRecordStreamFormat(csvFormat, new Path(path))
                .build();

        DataStream<CityRecord> dataStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "csv-source");
        dataStream
                .map(new MapFunction<CityRecord, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(CityRecord record) {
                        return Tuple2.of(record.getCountry(), 1);
                    }
                })
                .keyBy(0)
                .sum(1)
                .print();
        env.execute();
    }
}