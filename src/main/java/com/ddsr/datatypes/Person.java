package com.ddsr.datatypes;

/**
 * Flink recognizes a data type as a POJO type if all of the following conditions are met:
 * <ul>
 *   <li>The class is public and standalone (not a non-static inner class).</li>
 *   <li>The class has a public no-argument constructor.</li>
 *   <li>All non-static, non-transient fields in the class (and in all superclasses) must satisfy the following:
 *     <ul>
 *       <li>They are either public and non-final, or</li>
 *       <li>They have corresponding public getter and setter methods that follow the JavaBeans naming conventions.</li>
 *     </ul>
 *   </li>
 * </ul>
 *
 * TODO: Flink’s serializer supports schema evolution for POJO types
 */
public class Person {
    public String name;
    public Integer age;

    public Person() {
    }

    public Person(String name, Integer age) {
        this.name = name;
        this.age = age;
    }
}
