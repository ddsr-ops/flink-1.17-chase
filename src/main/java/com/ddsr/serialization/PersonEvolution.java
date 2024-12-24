package com.ddsr.serialization;

import java.util.Objects;

/**
 * This class aims to demonstrate the schema evolution for state. This class evolves from Person.
 *
 * @author ddsr, created it at 2024/12/20 17:00
 */
public class PersonEvolution {

    public String name;
    public String sex;
    public int age;  // new field added comparing with Person

    @Override
    public String toString() {
        return "Person{" +
                "name='" + name + '\'' +
                ", sex='" + sex + '\'' +
                ", age=" + age +
                '}';
    }

    public PersonEvolution(String name, String sex, int age) {
        this.name = name;
        this.sex = sex;
        this.age = age;
    }

    public PersonEvolution() {
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PersonEvolution that = (PersonEvolution) o;
        return age == that.age && Objects.equals(name, that.name) && Objects.equals(sex, that.sex);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, sex, age);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }
}
