package tms.realtime.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.codehaus.plexus.util.StringUtils;
import tms.realtime.app.func.DimSinkFunction;
import tms.realtime.app.func.MyBroadcastProcessFunction;
import tms.realtime.beans.TmsConfigDimBean;
import tms.realtime.common.TmsConfig;
import tms.realtime.utils.CreateEnvUtil;
import tms.realtime.utils.HBaseUtil;
import tms.realtime.utils.KafkaUtil;


/**
 * @author xiaojia
 * @date 2023/11/1 16:55
 * @desc DIM维度层处理
 */
public class DimApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //指定流处理环境以及检查点相关的设置
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(args);
        //设置并行度
        env.setParallelism(4);
        //TODO 2.从kafka的tms_ods主题中读取业务数据
        //声明消费的主题以及消费者组
        String topic="tms_ods";
        String groupId="dim_app_group";
        String sourceName="Kafka Source";
        //创建消费者对象
        KafkaSource<String> kafkaSource = KafkaUtil.getKafkaSource(topic, groupId,args);
        SingleOutputStreamOperator<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), sourceName)
                .uid("Kafka Source");



        //TODO 3.对读取的数据进行类型转换并过滤掉不需要传递的json属性
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String jsonStr) throws Exception {
                JSONObject jsonObject = JSON.parseObject(jsonStr);
                String table = jsonObject.getJSONObject("source").getString("table");
                jsonObject.put("table", table);
                jsonObject.remove("before");
                jsonObject.remove("source");
                jsonObject.remove("transaction");
                return jsonObject;
            }
        });
        //TODO 4.使用flink-cdc读取配置表数据
        MySqlSource<String> mySqlSource = CreateEnvUtil.getMySqlSource("config_dim", "6000", args);
        SingleOutputStreamOperator<String> mysqlDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source")
                .setParallelism(1)
                .uid("mysqlS_source");

        //TODO 5.提前将HBase中的维度表创建出来
        SingleOutputStreamOperator<String> createTableDS = mysqlDS.map(new MapFunction<String, String>() {
            @Override
            public String map(String jsonStr) throws Exception {
                JSONObject jsonObject = JSON.parseObject(jsonStr);
                String op = jsonObject.getString("op");
                if ("r".equals(op) || "c".equals(op)) {
                    JSONObject after = jsonObject.getJSONObject("after");
                    String sinkTable = after.getString("sink_table");
                    String sinkFamily = after.getString("sink_family");
                    if (StringUtils.isEmpty(sinkFamily)) {
                        sinkFamily = "info";
                    }
                    System.out.println("在HBase中创建" + sinkTable);
                    HBaseUtil.createTable(TmsConfig.HBASE_NAMESPACE, sinkTable, sinkFamily.split(","));
                }
                return jsonStr;
            }
        });

        //TODO 6.对配置数据进行广播
        MapStateDescriptor<String, TmsConfigDimBean> mapStateDescriptor
                = new MapStateDescriptor<String, TmsConfigDimBean>("mapStateDescriptor",String.class, TmsConfigDimBean.class);
        BroadcastStream<String> broadcastDS = createTableDS.broadcast(mapStateDescriptor);
        //TODO 7.将主流和广播流进行关联---connect
        BroadcastConnectedStream<JSONObject, String> connectDS = jsonObjDS.connect(broadcastDS);
        //TODO 8.对关联之后的数据进行处理
        SingleOutputStreamOperator<JSONObject> dimDS
                = connectDS.process(new MyBroadcastProcessFunction(mapStateDescriptor,args));
        //TODO 9.将维度数据保存到HBase维度表中

        dimDS.addSink(new DimSinkFunction());

        env.execute();
    }
}
