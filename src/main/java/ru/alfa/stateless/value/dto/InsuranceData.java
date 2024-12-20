package ru.alfa.stateless.value.dto;

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

