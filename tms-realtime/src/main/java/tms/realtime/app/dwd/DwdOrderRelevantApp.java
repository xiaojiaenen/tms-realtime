package tms.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;
import tms.realtime.utils.CreateEnvUtil;
import tms.realtime.utils.KafkaUtil;

/**
 * @author xiaojia
 * @date 2023/11/6 10:07
 * @desc 订单相关事实表准备
 */
public class DwdOrderRelevantApp {
    public static void main(String[] args) {
        //1、环境准备
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(args);
        env.setParallelism(4);
        //2、从kafka中的tms_ods中读取数据
        String topic = "tms_ods";
        String groupId = "dwd_order_relevant_group";
        KafkaSource<String> kafkaSource = KafkaUtil.getKafkaSource(topic, groupId, args);
        SingleOutputStreamOperator<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source")
                .uid("kafka_source");
        //3、筛选订单和订单明细数据
        SingleOutputStreamOperator<String> filterDS = kafkaStrDS.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String jsonStr) throws Exception {
                JSONObject jsonObject = JSON.parseObject(jsonStr);
                String tableName = jsonObject.getJSONObject("source").getString("table");

                return "order_info".equals(tableName) || "order_cargo".equals(tableName);
            }
        });
        //4、对流中的数据类型进行转换（jsonStr->jsonObj）
        SingleOutputStreamOperator<JSONObject> jsonObjDS = filterDS.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String jsonStr) throws Exception {
                JSONObject jsonObject = JSON.parseObject(jsonStr);
                String tableName = jsonObject.getJSONObject("source").getString("table");
                jsonObject.put("table", tableName);
                jsonObject.remove("source");
                jsonObject.remove("transaction");
                return jsonObject;
            }
        });
        //5、按照order_id进行分组
        KeyedStream<JSONObject, String> keyedDS = jsonObjDS.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
                String table = jsonObject.getString("table");
                if ("order_info".equals(table)) {
                    return jsonObject.getJSONObject("after").getString("id");
                }
                return jsonObject.getJSONObject("after").getString("order_id");
            }
        });
        //6、定义侧输出流标签 下单->主流
        //支付成功、取消运单、揽收、发单、转运成功、派送成功、签收->侧输出流
        // 支付成功明细流标签
        OutputTag<String> paySucTag = new OutputTag<String>("dwd_trade_pay_suc_detail") {};
        // 取消运单明细流标签
        OutputTag<String> cancelDetailTag = new OutputTag<String>("dwd_trade_cancel_detail") {};
        // 揽收明细流标签
        OutputTag<String> receiveDetailTag = new OutputTag<String>("dwd_trans_receive_detail") {};
        // 发单明细流标签
        OutputTag<String> dispatchDetailTag = new OutputTag<String>("dwd_trans_dispatch_detail") {};
        // 转运完成明细流标签
        OutputTag<String> boundFinishDetailTag = new OutputTag<String>("dwd_trans_bound_finish_detail") {};
        // 派送成功明细流标签
        OutputTag<String> deliverSucDetailTag = new OutputTag<String>("dwd_trans_deliver_detail") {};
        // 签收明细流标签
        OutputTag<String> signDetailTag = new OutputTag<String>("dwd_trans_sign_detail") {};
        //7、分流

        //8、从主流中提取侧输出流
        //9、将不同流的数据写到kafka的不同主题中
    }
}
