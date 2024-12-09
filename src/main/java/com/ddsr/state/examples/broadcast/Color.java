package com.ddsr.state.examples.broadcast;

import java.util.Objects;

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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Color color = (Color) o;
        return Objects.equals(name, color.name);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name);
    }

    @Override
    public String toString() {
        return "Color{" +
                "name='" + name + '\'' +
                '}';
    }
}