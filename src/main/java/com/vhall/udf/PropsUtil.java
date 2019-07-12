package com.vhall.udf;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

/**
 * @Title: PropsUtil
 * @ProjectName IpUdtf
 * @Description: TODO 加载配置信息的工具类
 * @Author yisheng.wu
 * @Date 2019/7/817:51
 */
public class PropsUtil implements Serializable {

    private Properties LoadProperties(String conf_file) {

        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(conf_file));
        } catch (IOException e) {
            e.printStackTrace();
        }

        return properties;
    }

    /**
      * @Author: yisheng.wu
      * @Description TODO 获取加载redis的配置信息
      * @Date 17:54 2019/7/8
      * @Param [path]
      * @return java.util.Properties
      **/
    public Properties getRedisProps(String path){

        Properties properties = LoadProperties(path);
        return properties;
    }
}
