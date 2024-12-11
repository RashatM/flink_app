package ru.alfa.sources.kafka.stateless;

public class InsuranceData {
    String insurer;
    String model;
    String insuranceType;

    public InsuranceData(String insurer, String model, String insuranceType) {
        this.insurer = insurer;
        this.model = model;
        this.insuranceType = insuranceType;
    }

    public String getInsurer() {
        return insurer;
    }

    public String getModel() {
        return model;
    }

    public String getInsuranceType() {
        return insuranceType;
    }
}

