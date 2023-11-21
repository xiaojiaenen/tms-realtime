package tms.realtime.app.func;

/**
 * @author xiaojia
 * @date 2023/11/16 20:15
 * @desc 维度关联实现的接口
 */

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;

public interface DimJoinFunction<T> {
    //补充维度属性到流中对象上
    void join(T obj, JSONObject dimJsonObj);

    //获取维度主键
    Tuple2<String,String> getCondition(T obj);
}