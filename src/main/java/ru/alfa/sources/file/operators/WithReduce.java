package ru.alfa.sources.file.operators;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import ru.alfa.sources.file.dto.CountryCount;

public class WithReduce {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> dataStream = env.readTextFile("/Users/rmusin/IdeaProjects/flink_app/target/classes/wpop.csv");
        DataStream<CountryCount> countryCounts = dataStream
                .map(new MapFunction<String, CountryCount>() {
                    @Override
                    public CountryCount map(String line) throws Exception {
                        String[] fields = line.split(",");
                        String country = fields[0]; // Извлекаем страну из первой колонки
                        return new CountryCount(country, 1); // Возвращаем объект
                    }
                })
                .keyBy(CountryCount::getCountry) // Группируем по стране
                .reduce(
                        (value1, value2) -> new CountryCount(
                                value1.getCountry(),
                                value1.getCount() + value2.getCount()
                        )
                ); // Суммируем количество
        env.execute();




    }
}