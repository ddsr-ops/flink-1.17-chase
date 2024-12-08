package com.ddsr.state.examples.broadcast;

/**
 * @author ddsr, created it at 2024/12/7 20:25
 */
// Item class
public class Item {
    private String id;
    private Shape shape;

    public Item() { }

    public Item(String id, Shape shape) {
        this.id = id;
        this.shape = shape;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Shape getShape() {
        return shape;
    }

    public void setShape(Shape shape) {
        this.shape = shape;
    }

    @Override
    public String toString() {
        return "Item{" +
                "id='" + id + '\'' +
                ", shape=" + shape +
                '}';
    }
}
