package com.stream.utils;

import com.stream.common.utils.ConfigUtils;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @Package:
 * @Author: lz
 * @Date: 2025/8/15 14:51
 * @version: 1.8
 */
public class HiveCatalogUtils {
    private static final String HIVE_CONF_DIR = ConfigUtils.getString("hive.conf.dir");

    public static HiveCatalog getHiveCatalog(String catalogName){
        System.setProperty("HADOOP_USER_NAME","root");
        return new HiveCatalog(catalogName, "default", HIVE_CONF_DIR);
    }
}
