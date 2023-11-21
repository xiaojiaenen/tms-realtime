package tms.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import tms.realtime.common.TmsConfig;
import tms.realtime.utils.DimUtil;
import tms.realtime.utils.ThreadPoolUtil;

import java.util.Collections;
import java.util.concurrent.ExecutorService;

/**
 * @author xiaojia
 * @date 2023/11/16 20:16
 * @desc 发送异步请求进行维度关联实现的接口
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimJoinFunction<T> {
    // 表名
    private String tableName;
    // 线程池操作对象
    private ExecutorService executorService;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        executorService = ThreadPoolUtil.getInstance();
    }

    @Override
    public void asyncInvoke(T obj, ResultFuture<T> resultFuture) throws Exception {
        //1.从线程池中获取线程，发送异步请求
        executorService.submit(
                new Runnable() {
                    @Override
                    public void run() {
                        //2. 根据流中的对象封装要作为查询条件的主键或者外键以及对应的值
                        Tuple2<String,String> keyNameAndValue = getCondition(obj);
                        //3. 根据维度的主键获取维度对象
                        JSONObject dimJsonObj = DimUtil.getDimInfo(TmsConfig.HBASE_NAMESPACE, tableName, keyNameAndValue);
                        //4. 将查询出来的维度信息 补充到流中的对象属性上
                        if (dimJsonObj != null) {
                            join(obj, dimJsonObj);
                        }
                        //5. 向下游传递数据
                        resultFuture.complete(Collections.singleton(obj));
                    }
                }
        );
    }
}