package tms.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import tms.realtime.beans.DwdBoundSortBean;
import tms.realtime.beans.DwsBoundOrgSortDayBean;
import tms.realtime.utils.CreateEnvUtil;
import tms.realtime.utils.KafkaUtil;

/**
 * @author xiaojia
 * @date 2023/11/14 15:49
 * @desc 中转域：机构粒度分拣业务过程聚合统计
 */
public class DwsBoundOrgSortDay {
    public static void main(String[] args) throws Exception {
        //1、环境处理
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(args);
        env.setParallelism(4);
        //2、从kafka中读取数据
        String topic = "tms_dwd_bound_sort";
        String groupId = "dwd_bound_org_sort_group";
        KafkaSource<String> kafkaSource = KafkaUtil.getKafkaSource(topic, groupId, args);
        SingleOutputStreamOperator<String> kafkaSourceDS
                = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source").uid("kafka_source");
        //3、对流中的数据进行类型转换 jsonStr=>实体类
        SingleOutputStreamOperator<DwsBoundOrgSortDayBean> boundOrgSortDayBeanDS = kafkaSourceDS.map(
                new MapFunction<String, DwsBoundOrgSortDayBean>() {
                    @Override
                    public DwsBoundOrgSortDayBean map(String jsonStr) throws Exception {
                        DwdBoundSortBean dwdBoundSortBean = JSON.parseObject(jsonStr, DwdBoundSortBean.class);
                        DwsBoundOrgSortDayBean sortDayBean = DwsBoundOrgSortDayBean.builder()
                                .orgId(dwdBoundSortBean.getOrgId())
                                .sortCountBase(1L)
                                .ts(dwdBoundSortBean.getTs()+8*60*60*1000L)
                                .build();
                        return sortDayBean;
                    }
                });
        boundOrgSortDayBeanDS.print(">>>");
        //4、指定Watermark以及提取事件时间字段
        SingleOutputStreamOperator<DwsBoundOrgSortDayBean> withWatermarkDS = boundOrgSortDayBeanDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<DwsBoundOrgSortDayBean>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<DwsBoundOrgSortDayBean>() {
                            @Override
                            public long extractTimestamp(DwsBoundOrgSortDayBean dwsBoundOrgSortDayBean, long l) {
                                return dwsBoundOrgSortDayBean.getTs();
                            }
                        })
        );
        //5、按照机构id进行分组
        KeyedStream<DwsBoundOrgSortDayBean, String> keyedDS = withWatermarkDS.keyBy(DwsBoundOrgSortDayBean::getOrgId);
        //6、开窗
        WindowedStream<DwsBoundOrgSortDayBean, String, TimeWindow> windowDS
                = keyedDS.window(TumblingEventTimeWindows.of(Time.days(1L)));
        //7、指定自定义的触发器

        //8、聚合
        //9、关联维度（城市、省份）
        //10、将关联的结果写到ck表中

        env.execute();
    }
}
