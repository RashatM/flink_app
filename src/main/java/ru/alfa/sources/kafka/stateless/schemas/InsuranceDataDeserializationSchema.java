package ru.alfa.sources.kafka.stateless.schemas;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import ru.alfa.sources.kafka.stateless.dto.InsuranceData;

import java.io.IOException;

public class InsuranceDataDeserializationSchema implements DeserializationSchema<InsuranceData> {

    @Override
    public InsuranceData deserialize(byte[] message) throws IOException {
        String line = new String(message); // Преобразуем байты в строку
        String[] fields = line.split(";");

        String insurer = fields[0].split("=")[1];
        String model = fields[1].split("=")[1];
        String insuranceType = fields[2];
        return new InsuranceData(insurer, model, insuranceType);
    }

    @Override
    public boolean isEndOfStream(InsuranceData nextElement) {
        return false; // Указываем, что поток данных не имеет конца
    }

    @Override
    public TypeInformation<InsuranceData> getProducedType() {
        return TypeInformation.of(InsuranceData.class); // Возвращаем тип данных, который будет производиться
    }
}
