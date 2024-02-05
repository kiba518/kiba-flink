package org.kiba.tools.job002;

import lombok.Data;
import lombok.ToString;

@ToString
@Data
public class Test_LogInfo {
    private int id;
    private String loginfo;
    private long create_timestamp;
}