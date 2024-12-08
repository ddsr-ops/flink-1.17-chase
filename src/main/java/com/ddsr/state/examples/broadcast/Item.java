package com.ddsr.state.examples.broadcast;

/**
 * @author ddsr, created it at 2024/12/7 20:25
 */
// Item class
public class Item {
    private Shape shape;
    private Color color;

    public Item() { }

    public Item(Color color, Shape shape) {
        this.shape = shape;
        this.color = color;
    }

    public Shape getShape() {
        return shape;
    }

    public void setShape(Shape shape) {
        this.shape = shape;
    }

    public Color getColor() {
        return color;
    }

    @Override
    public String toString() {
        return "Item{" +
                "shape=" + shape +
                ", color=" + color +
                '}';
    }

    public void setColor(Color color) {
        this.color = color;
    }
}
