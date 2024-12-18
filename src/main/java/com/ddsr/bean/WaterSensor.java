package com.ddsr.bean;

import java.util.Objects;

/**
 * POJOs:
 * <li>The class must be public..</li>
 *
 * <li>It must have a public constructor without arguments (default constructor).</li>
 *
 * <li>All fields are either public or must be accessible through getter and setter functions. For a field called foo
 * the getter and setter methods must be named getFoo() and setFoo().</li>
 *
 * <li>The type of a field must be supported by a registered serializer</li>
 * @author ddsr, created it at 2023/8/18 21:40
 */
public class WaterSensor {
    // sensor id
    public String id;
    // timestamp
    public Long ts;
    // vc = watermark?
    public Integer vc;

    public WaterSensor() {
    }

    public WaterSensor(String id, Long ts, Integer vc) {
        this.id = id;
        this.ts = ts;
        this.vc = vc;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public Integer getVc() {
        return vc;
    }

    public void setVc(Integer vc) {
        this.vc = vc;
    }

    @Override
    public String toString() {
        return "WaterSensor{" +
                "id='" + id + '\'' +
                ", ts=" + ts +
                ", vc=" + vc +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        WaterSensor that = (WaterSensor) o;
        return Objects.equals(id, that.id) &&
                Objects.equals(ts, that.ts) &&
                Objects.equals(vc, that.vc);
    }

    @Override
    public int hashCode() {

        return Objects.hash(id, ts, vc);
    }
}