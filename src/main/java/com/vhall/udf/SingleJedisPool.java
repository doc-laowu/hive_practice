package com.vhall.udf;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.Serializable;
import java.util.Properties;

/**
 * @Copyright(C) 北京微吼时代科技有限公司
 * @WebSite http://www.vhall.com/
 * @ClassName SingleJedisPool
 * @Description 单例redis连接池
 * @Version V1.0.0
 * @Date 2019/4/2 16:23
 * @Author yisheng.wu
 */
public class SingleJedisPool implements Serializable {

    private volatile static SingleJedisPool singleton;

    // redis 连接池
    private static JedisPool jedisPool = null;

    private SingleJedisPool(Properties redisPro) throws Exception {
        try {
            JedisPoolConfig config = new JedisPoolConfig();
            String address = redisPro.getProperty("redis.address");
            Integer port = Integer.parseInt(redisPro.getProperty("redis.port"));
            String password = redisPro.getProperty("redis.password") == null ? null : redisPro.getProperty("redis.password");
            Integer timeout = Integer.parseInt(redisPro.getProperty("redis.timeout"));
            Integer maxtotal = Integer.parseInt(redisPro.getProperty("redis.maxtotal"));
            Integer maxidle = Integer.parseInt(redisPro.getProperty("redis.maxidle"));
            Long maxwait = Long.parseLong(redisPro.getProperty("redis.maxwait"));
            Boolean testOnBorrow = "true".equals(redisPro.getProperty("redis.testOnBorrow").toLowerCase()) ? true : false;
            Boolean testOnReturn = "true".equals(redisPro.getProperty("redis.testOnReturn").toLowerCase()) ? true : false;
            config.setMaxTotal(maxtotal);
            config.setMaxIdle(maxidle);
            config.setMaxWaitMillis(maxwait);
            config.setTestOnBorrow(testOnBorrow);
            config.setTestOnReturn(testOnReturn);
            jedisPool = new JedisPool(config, address, port, timeout, password);

        } catch (Exception e) {
            throw new RuntimeException("create redis connect pool error!" + e.getMessage());
        }
    }

    /**
      * @Author: yisheng.wu
      * @Description TODO 销毁连接池
      * @Date 11:23 2019/7/9
      * @Param []
      * @return void
      **/
    public static void destoryPool(){
        jedisPool.destroy();
    }

    private static SingleJedisPool getSingleton(Properties redisPro) throws Exception {
        if (singleton == null) {
            synchronized (SingleJedisPool.class) {
                if (singleton == null) {
                    singleton = new SingleJedisPool(redisPro);
                }
            }
        }
        return singleton;
    }


    // 获取 Jedis 实例
    public static Jedis getJedis(Properties redisPro) throws Exception {

        Jedis jedis = getSingleton(redisPro).jedisPool.getResource();
        jedis.select(Integer.parseInt(redisPro.getProperty("redis.dbnum")));
        return jedis;
    }
}
