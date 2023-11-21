package tms.realtime.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @author xiaojia
 * @date 2023/11/15 21:36
 * @desc
 */
public class JedisUtil {

    private static JedisPool jedisPool;
    static {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setBlockWhenExhausted(true);
        poolConfig.setMaxWaitMillis(2000L);
        poolConfig.setMaxTotal(1000);
        poolConfig.setMaxIdle(5);
        poolConfig.setMinIdle(5);
        poolConfig.setTestOnBorrow(true);
        jedisPool=new JedisPool(poolConfig,"hadoop101",6379,10000);
    }

    public static Jedis getJedis(){
        System.out.println("====创建Jedis客户端====");
        return jedisPool.getResource();
    }
}
