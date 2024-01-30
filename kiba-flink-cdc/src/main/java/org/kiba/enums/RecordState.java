package org.kiba.enums;

public enum RecordState {
    读取("read"),
    插入("insert"),
    更新("update"),
    删除("delete");


    private String value;
    RecordState(String state) {
        this.value = state;
    }
    public String getValue(){
        return value;
    }
}
