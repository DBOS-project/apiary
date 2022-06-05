package org.dbos.apiary.procedures.elasticsearch;

import org.dbos.apiary.elasticsearch.ApiaryDocument;

public class Person extends ApiaryDocument {
    private String name;
    private int number;

    public Person() {}
    public Person(String name, int number) {
        this.name = name;
        this.number = number;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getNumber() {
        return number;
    }

    public void setNumber(int number) {
        this.number = number;
    }
}
