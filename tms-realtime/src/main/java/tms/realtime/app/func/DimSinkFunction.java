package tms.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.hadoop.hbase.client.Put;
import tms.realtime.common.TmsConfig;
import tms.realtime.utils.HBaseUtil;

import java.util.Map;
import java.util.Set;

/**
 * @author xiaojia
 * @date 2023/11/2 17:11
 * @desc 将维度流中的数据写到HBase中
 */
public class DimSinkFunction implements SinkFunction<JSONObject> {
    @Override
    public void invoke(JSONObject jsonObject, Context context) throws Exception {
        String sinkTable = jsonObject.getString("sink_table");
        jsonObject.remove("sink_table");
        String sinkPk = jsonObject.getString("sink_pk");
        jsonObject.remove("sink_pk");
        Set<Map.Entry<String, Object>> entrySet = jsonObject.entrySet();
        Put put = new Put(jsonObject.getString(sinkPk).getBytes());
        for (Map.Entry<String, Object> entry : entrySet) {
            if (!sinkPk.equals(entry.getKey())) {
                put.addColumn("info".getBytes(), entry.getKey().getBytes(), entry.getValue().toString().getBytes());
            }
        }
        //向Hbase表中插入一行数据（多个cell）
        System.out.println("向hbase表中插入数据");
        HBaseUtil.putRow(TmsConfig.HBASE_NAMESPACE, sinkTable, put);
    }
}
