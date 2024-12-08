package com.ddsr.state.examples.broadcast;

/**
 * @author ddsr, created it at 2024/12/8 15:54
 */
public class Color {
    private String name;

    public Color() { }

    public Color(String name) {
        this.name = name;
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