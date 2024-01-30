package org.kiba.config;

import lombok.Data;

@Data
public class Config {

    private String middle_database_url;
    private String middle_database_username;
    private String middle_database_password;


    /**
     * 主机和端口
     */
    private String host;
    private int port;
    private String db_name;
}
