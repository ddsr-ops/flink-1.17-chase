package com.ddsr.state.serialization;

import java.io.*;

/**
 * Serialize and deserialize an object through Java serialization
 *
 * @author ddsr, created it at 2024/12/30 14:20
 */
@SuppressWarnings({"unused", "FieldCanBeLocal", "IOStreamConstructor"})
public class UserWithJavaSerialization implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String name;
    private final int age;

    public UserWithJavaSerialization(String name, int age) {
        this.name = name;
        this.age = age;
    }

    public static void serializeUser(UserWithJavaSerialization user) {
        try (ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream("user.ser"))){
            out.writeObject(user);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static UserWithJavaSerialization deserializeUser() {
        try (ObjectInputStream in = new ObjectInputStream(new FileInputStream("user.ser"))){
            return (UserWithJavaSerialization) in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

}
