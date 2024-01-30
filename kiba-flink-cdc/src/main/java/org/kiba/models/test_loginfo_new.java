package org.kiba.models;


import lombok.Data;
import lombok.ToString;

import java.util.Date;

@Data
@ToString
public class test_loginfo_new {

    private Integer id = 0;

    private String loginfo;

    private Long create_timestamp;
}
