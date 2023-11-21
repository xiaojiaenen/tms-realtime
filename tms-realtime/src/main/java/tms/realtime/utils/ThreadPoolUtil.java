package tms.realtime.utils;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author xiaojia
 * @date 2023/11/16 10:45
 * @desc 获取线程池工具类
 * 通过双重校验锁解决单例设计模式懒汉式线程安全问题
 */
public class ThreadPoolUtil {
    private static volatile ThreadPoolExecutor poolExecutor;

    public static ThreadPoolExecutor getInstance() {
        if (poolExecutor == null) {
            synchronized (ThreadPoolUtil.class) {
                if(poolExecutor == null) {
                    System.out.println("====创建线程池====");
                    poolExecutor = new ThreadPoolExecutor(
                            4,
                            20,
                            300,
                            TimeUnit.SECONDS,
                            new LinkedBlockingDeque<Runnable>(Integer.MAX_VALUE)
                    );
                }
            }
        }
        return poolExecutor;
    }
}
