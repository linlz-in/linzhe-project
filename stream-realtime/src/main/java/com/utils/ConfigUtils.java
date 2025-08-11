package com.utils;

import org.apache.parquet.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * @Package:
 * @Author: lz
 * @Date: 2025/8/11 20:38
 * @version: 1.8
 */
public class ConfigUtils {
    private static final Logger  logger= LoggerFactory.getLogger(ConfigUtils.class);
    private static Properties properties;
    static {
        try {
            properties = new Properties();
            properties.load(ConfigUtils.class.getClassLoader().getResourceAsStream("common-config.propertie"));
        } catch (IOException e) {
            logger.error("加载配置文件出错，exit 1",e);
            System.exit(1);
        }
    }

    public static String getString(String key){
        return properties.getProperty(key).trim();
    }

    public static int getInt(String key){
        String value = properties.getProperty(key).trim();
        return Integer.parseInt(value);
    }

    public static int getInt(String key,int defaultValue){
        String value = properties.getProperty(key).trim();
        return Strings.isNullOrEmpty( value) ? defaultValue : Integer.parseInt(value);
    }

    public static long getLong(String key){
        String value = properties.getProperty(key).trim();
        return Long.parseLong(value);
    }

    public static long getLong(String key,long defaultValue){
        String value = properties.getProperty(key).trim();
        return Strings.isNullOrEmpty( value) ? defaultValue : Long.parseLong(value);
    }
}
