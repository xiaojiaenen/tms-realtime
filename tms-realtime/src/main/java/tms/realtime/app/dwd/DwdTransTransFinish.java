package tms.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import tms.realtime.beans.DwdTransTransFinishBean;
import tms.realtime.utils.CreateEnvUtil;
import tms.realtime.utils.DateFormatUtil;
import tms.realtime.utils.KafkaUtil;

/**
 * @author xiaojia
 * @date 2023/11/10 08:14
 * @desc 物流域：运输完成事实表
 */
public class DwdTransTransFinish {
    public static void main(String[] args) throws Exception {
        //1、准备环境
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(args);
        env.setParallelism(4);
        //2、从kafka tms_ods主题中读取数据
        String topic = "tms_ods";
        String groupId = "dwd_trans_trans_finish_group";
        KafkaSource<String> kafkaSource = KafkaUtil.getKafkaSource(topic, groupId, args);
        SingleOutputStreamOperator<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source")
                .uid("kafka_source");
        //3、筛选出运输完成的数据
        SingleOutputStreamOperator<String> filteredStream = kafkaStrDS.filter(
                new FilterFunction<String>() {
                    @Override
                    public boolean filter(String jsonStr) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        String table = jsonObj.getJSONObject("source").getString("table");
                        if (!"transport_task".equals(table)) {
                            return false;
                        }

                        String op = jsonObj.getString("op");
                        JSONObject before = jsonObj.getJSONObject("before");
                        if (before == null) {
                            return false;
                        }
                        JSONObject after = jsonObj.getJSONObject("after");
                        String oldActualEndTime = before.getString("actual_end_time");
                        String actualEndTime = after.getString("actual_end_time");
                        return "u".equals(op) &&
                                oldActualEndTime == null &&
                                actualEndTime != null;
                    }
                }
        );
        //4、对筛选出的数据进行处理（时间问题修复、脱敏、ts指定）
        SingleOutputStreamOperator<String> processedStream = filteredStream.process(
                new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String jsonStr, Context context, Collector<String> out) throws Exception {
                        // 获取修改后的数据并转换数据类型
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        DwdTransTransFinishBean dwdTransTransFinishBean =
                                jsonObj.getObject("after", DwdTransTransFinishBean.class);

                        // 补全时间戳字段
                        dwdTransTransFinishBean.setTs(
                                Long.parseLong(dwdTransTransFinishBean.getActualEndTime())
                                        - 8 * 60 * 60 * 1000L
                        );

                        // 补全运输时长字段
                        dwdTransTransFinishBean.setTransportTime(
                                Long.parseLong(dwdTransTransFinishBean.getActualEndTime())
                                        - Long.parseLong(dwdTransTransFinishBean.getActualStartTime())
                        );

                        // 处理时区问题
                        dwdTransTransFinishBean.setActualStartTime(
                                DateFormatUtil.toYmdHms(
                                        Long.parseLong(dwdTransTransFinishBean.getActualStartTime())
                                                - 8 * 60 * 60 * 1000L));

                        dwdTransTransFinishBean.setActualEndTime(
                                DateFormatUtil.toYmdHms(
                                        Long.parseLong(dwdTransTransFinishBean.getActualEndTime())
                                                - 8 * 60 * 60 * 1000L));

                        // 脱敏
                        String driver1Name = dwdTransTransFinishBean.getDriver1Name();
                        String driver2Name = dwdTransTransFinishBean.getDriver2Name();
                        String truckNo = dwdTransTransFinishBean.getTruckNo();

                        driver1Name = driver1Name.charAt(0) +
                                driver1Name.substring(1).replaceAll(".", "\\*");
                        driver2Name = driver2Name == null ? driver2Name : driver2Name.charAt(0) +
                                driver2Name.substring(1).replaceAll(".", "\\*");
                        truckNo = DigestUtils.md5Hex(truckNo);

                        dwdTransTransFinishBean.setDriver1Name(driver1Name);
                        dwdTransTransFinishBean.setDriver2Name(driver2Name);
                        dwdTransTransFinishBean.setTruckNo(truckNo);

                        out.collect(JSON.toJSONString(dwdTransTransFinishBean));
                    }
                }
        );
        //5、将处理后的数据写到kafka主题中
        String sinkTopic = "tms_dwd_trans_trans_finish";
        KafkaSink<String> kafkaProducer = KafkaUtil.getKafkaSink(sinkTopic, args);
        processedStream
                .sinkTo(kafkaProducer)
                .uid("data_kafka_sink");

        env.execute();
    }

}
