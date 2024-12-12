package ru.alfa.stateless.value.schemas;


import org.apache.flink.api.common.serialization.SerializationSchema;
import ru.alfa.stateless.value.dto.InsuranceData;

public class InsuranceDataSerializationSchema implements SerializationSchema<InsuranceData> {

    @Override
    public byte[] serialize(InsuranceData insuranceData) {
        // Преобразуем объект в строку формата "insurer=value;model=value;insuranceType=value"
        String serialized = "insurer=" + insuranceData.getInsurer() + ";"
                + "model=" + insuranceData.getModel() + ";"
                + "insuranceType=" + insuranceData.getInsuranceType();
        return serialized.getBytes();
    }
}

