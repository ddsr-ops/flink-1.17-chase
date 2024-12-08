package com.ddsr.state.examples.broadcast;

/**
 * @author ddsr, created it at 2024/12/8 15:55
 */
public class Rule {
    public String name;
    public Shape first;
    public Shape second;

    public Rule() { }

    public Rule(String name, Shape first, Shape second) {
        this.name = name;
        this.first = first;
        this.second = second;
    }

    @Override
    public String toString() {
        return "Rule{" +
                "name='" + name + '\'' +
                ", first=" + first +
                ", second=" + second +
                '}';
    }
}
