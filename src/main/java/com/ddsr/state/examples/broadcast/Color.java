package com.ddsr.state.examples.broadcast;

/**
 * @author ddsr, created it at 2024/12/8 15:54
 */
public class Color {
    private String name;

    private Color(String name) {
        this.name = name;
    }

    public static Color valueOf(String color) {
        return new Color(color);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "Color{" +
                "name='" + name + '\'' +
                '}';
    }
}