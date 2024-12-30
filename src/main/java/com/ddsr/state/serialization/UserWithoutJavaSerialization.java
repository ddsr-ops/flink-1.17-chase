package com.ddsr.state.serialization;

import java.io.*;
import java.lang.reflect.InvocationTargetException;

/**
 * Instead of serializing the whole object, we'll only serialize the class name and recreate the object dynamically when
 * needed. This example does not use Java's built-in serialization directly for the object's state.
 *
 * @author ddsr, created it at 2024/12/30 14:22
 */
@SuppressWarnings("unused")
public class UserWithoutJavaSerialization {
    private final String name;
    private final int age;

    public UserWithoutJavaSerialization(String name, int age) {
        this.name = name;
        this.age = age;
    }

    public UserWithoutJavaSerialization() {
        this.name = "tft";
        this.age = 8;
    }

    public static void saveClassName(Object obj) {
        try (PrintWriter out = new PrintWriter(new FileOutputStream("userClassName.txt"))) {
            // only serialize the class name
            out.println(obj.getClass().getName());
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static Object recreateObject() {
        try (BufferedReader reader = new BufferedReader(new FileReader("userClassName.txt"))) {
            String className = reader.readLine();
            Class<?> clazz = Class.forName(className);
            //  When needed, we dynamically recreate the object using reflection
            return clazz.getDeclaredConstructor().newInstance();
        } catch (IOException | InvocationTargetException | InstantiationException | NoSuchMethodException |
                 ClassNotFoundException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

}
