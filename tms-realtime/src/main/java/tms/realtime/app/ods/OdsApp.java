package tms.realtime.app.ods;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.esotericsoftware.minlog.Log;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import tms.realtime.utils.CreateEnvUtil;
import tms.realtime.utils.KafkaUtil;

/**
 * @author xiaojia
 * @date 2023/10/31 20:21
 * @desc ODS数据的采集
 */
public class OdsApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(args);
        env.setParallelism(4);

        //事实数据
        String dwdOption = "dwd";
        String dwdServerId = "6030";
        String dwdSourceName = "ods_app_dwd_source";
        //维度数据
        String realtimeDimOption = "realtime_dim";
        String realtimeDimServerId = "6040";
        String realtimeDimSourceName = "ods_app_realtimeDim_source";

        mysqlToSink(dwdOption,dwdServerId,dwdSourceName,env,args);
        mysqlToSink(realtimeDimOption,realtimeDimServerId,realtimeDimSourceName,env,args);

        env.execute();
    }

    public static void mysqlToSink(String option, String serverId, String sourceName, StreamExecutionEnvironment env, String[] args){
        MySqlSource<String> dwdMySqlSource = CreateEnvUtil.getMySqlSource(option, serverId, args);


        SingleOutputStreamOperator<String> strDS = env
                .fromSource(dwdMySqlSource, WatermarkStrategy.noWatermarks(), sourceName)
                .setParallelism(1)
                .uid(option + sourceName);

        SingleOutputStreamOperator<String> processDS = strDS.process(
                        new ProcessFunction<String, String>() {
                            @Override
                            public void processElement(String jsonStr, ProcessFunction<String, String>.Context context, Collector<String> collector) throws Exception {
                                try {
                                    JSONObject jsonObject = JSON.parseObject(jsonStr);
                                    if (jsonObject.getJSONObject("after") != null && !jsonObject.getString("op").equals("d")) {
                                        Long tsMs = jsonObject.getLong("ts_ms");
                                        jsonObject.put("ts", tsMs);
                                        jsonObject.remove("ts_ms");
                                        collector.collect(jsonObject.toJSONString());
                                    }
                                } catch (Exception e) {
                                    e.printStackTrace();
                                    Log.error("从flink-cdc得到的数据不是一个标准的JSON格式");
                                }


                            }
                        }
                )
                .setParallelism(1);

        //按照主键进行分组，避免出现乱序
        KeyedStream<String, String> keyedDS = processDS.keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String jsonStr) throws Exception {
                JSONObject jsonObject = JSON.parseObject(jsonStr);
                return jsonObject.getJSONObject("after").getString("id");
            }
        });

        //将数据写入kafka主题中
        keyedDS.sinkTo(KafkaUtil.getKafkaSink("tms_ods",sourceName+"transPre",args))
                .uid(option+"_ods_app_sink");


    }

}
