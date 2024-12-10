package ru.alfa.sources.file;

public class CountryCount {
    public String country;
    public int count;

    public CountryCount(String country, int count) {
        this.country = country;
        this.count = count;
    }

    public String getCountry() {
        return country;
    }

    public int getCount() {
        return count;
    }
}
