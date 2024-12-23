package com.ddsr.datatypes;

import org.apache.flink.api.common.typeinfo.TypeInfo;

/**
 * A demonstration for a custom typeinfo factory
 */

@TypeInfo(Person1TypeFactory.class)
@SuppressWarnings("unused")
public class Person1 {
    private String name;
    private int age;

    // Default constructor is necessary for Flink's POJO type extraction
    public Person1() {}

    public Person1(String name, int age) {
        this.name = name;
        this.age = age;
    }

    // Getters and setters
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return "Person{" +
                "name='" + name + '\'' +
                ", age=" + age +
                '}';
    }
}
