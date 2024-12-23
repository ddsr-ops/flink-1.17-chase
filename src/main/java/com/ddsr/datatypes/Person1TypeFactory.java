package com.ddsr.datatypes;

import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.lang.reflect.Type;
import java.util.Map;

public class Person1TypeFactory extends TypeInfoFactory<Person> {
    @Override
    public TypeInformation<Person> createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParameters) {
        return TypeExtractor.createTypeInfo(Person.class);
    }
}