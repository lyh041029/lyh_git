package com.stream.utils;

import com.stream.common.utils.ConfigUtils;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @BelongsProject: lyh_git
 * @BelongsPackage: com.stream.utils
 * @Author: liyuhuan
 * @CreateTime: 2025-08-18  19:10
 * @Description: TODO
 * @Version: 1.0
 */
public class HiveCatalogUtils {
    private static final String HIVE_CONF_DIR = ConfigUtils.getString("hive.conf.dir");

    public static HiveCatalog getHiveCatalog(String catalogName){
        System.setProperty("HADOOP_USER_NAME","root");
        return new HiveCatalog(catalogName, "default", HIVE_CONF_DIR);
    }
}
