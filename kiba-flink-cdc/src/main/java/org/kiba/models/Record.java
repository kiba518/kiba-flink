package org.kiba.models;

import lombok.Data;

@Data
public class Record {

    private String database;
    private String tableName;
    private test_loginfo_new before;
    private test_loginfo_new after;
    private String type;
    private String ts;
}
