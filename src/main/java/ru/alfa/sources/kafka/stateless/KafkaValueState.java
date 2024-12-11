package ru.alfa.sources.kafka.stateless;


import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import ru.alfa.sources.kafka.stateless.dto.InsuranceData;

public class KafkaValueState extends RichFlatMapFunction<InsuranceData, InsuranceData> {
    private transient ValueState<Integer> requestCount;

    @Override
    public void flatMap(InsuranceData insuranceData, Collector<InsuranceData> collector) throws Exception {
        Integer currentCount = requestCount.value();

        // Если количество заявок больше 1, значит, это повторная заявка
        if (currentCount == 1) {
            collector.collect(insuranceData); // Отправляем повторную заявку
        }

        // Обновляем счетчик и больше не изменяем, что оптимизирует состояние
        requestCount.update( 1);

    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        ValueStateDescriptor<Integer> requestsDescriptor =
                new ValueStateDescriptor<>(
                        "requestCount", // the state name
                        TypeInformation.of(new TypeHint<Integer>() {}), // type information
                        0); // default value of the state, if nothing was set
        requestCount = getRuntimeContext().getState(requestsDescriptor);

    }
}


