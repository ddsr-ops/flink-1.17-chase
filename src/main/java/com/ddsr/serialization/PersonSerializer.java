package com.ddsr.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * The orders of read and write must be the same
 *
 * @author ddsr, created it at 2024/12/20 17:03
 */
public class PersonSerializer extends Serializer<Person> {
    @Override
    public void write(Kryo kryo, Output output, Person person) {
        output.writeString(person.getName()); // first
        output.writeString(person.getSex()); // second
    }

    @Override
    public Person read(Kryo kryo, Input input, Class<Person> aClass) {
        String name = input.readString(); // read first to name
        String sex = input.readString(); // read second to sex
        return new Person(name, sex);
    }
}
