package org.kiba.utils;

import cn.hutool.core.convert.Convert;
import cn.hutool.core.io.resource.ClassPathResource;
import cn.hutool.core.util.ClassUtil;
import lombok.extern.slf4j.Slf4j;
import org.kiba.config.Config;

import java.io.IOException;
import java.util.Calendar;
import java.util.Properties;

@Slf4j
public class ConfigUtils {

    public static volatile Config CONFIG = null;

    public static void init() throws IOException {

        ClassLoader classLoader = ClassUtil.getClassLoader();
        ClassPathResource resourceProfile = new ClassPathResource("profile.properties", classLoader);
        Properties propertiesProfile = new Properties();
        propertiesProfile.load(resourceProfile.getStream());
        String activeProfile = Convert.toStr(propertiesProfile.get("active"));

        ClassPathResource resource = new ClassPathResource(activeProfile + ".properties", classLoader);
        Properties properties = new Properties();
        properties.load(resource.getStream());
        String message = properties.getProperty("message");
        CONFIG = null;
        CONFIG = new Config();


        CONFIG.setMiddle_database_url(Convert.toStr(properties.get("middle-database.url")));
        CONFIG.setMiddle_database_username(Convert.toStr(properties.get("middle-database.username")));
        CONFIG.setMiddle_database_password(Convert.toStr(properties.get("middle-database.password")));
        CONFIG.setDb_name(Convert.toStr(properties.get("db_name")));

        CONFIG.setHost(Convert.toStr(properties.get("host")));
        CONFIG.setPort(Convert.toInt(properties.get("port")));
        log.info("===========当前的config============:" + CONFIG);
    }

}
