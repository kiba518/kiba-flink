package org.kiba.tools.job004;

public class ClickEvent {
    public ClickEvent(String name, int data, long timestamp) {
        this.name = name;
        this.data = data;
        this.timestamp = timestamp;
    }

    public String name;

    public int data;
    public long timestamp;

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public int getData() {
        return data;
    }

    public void setData(int data) {
        this.data = data;
    }


    @Override
    public String toString() {
        return name + " " + data + " " + timestamp;
    }
}
