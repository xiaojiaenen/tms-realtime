package tms.realtime.app.dws;

/**
 * @author xiaojia
 * @date 2023/11/17 15:35
 * @desc
 */
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import tms.realtime.app.func.DimAsyncFunction;
import tms.realtime.app.func.MyAggregationFunction;
import tms.realtime.app.func.MyTriggerFunction;
import tms.realtime.beans.DwdTransTransFinishBean;
import tms.realtime.beans.DwsTransOrgTruckModelTransFinishDayBean;
import tms.realtime.utils.ClickHouseUtil;
import tms.realtime.utils.CreateEnvUtil;
import tms.realtime.utils.DateFormatUtil;
import tms.realtime.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * 物流域机构卡车类别粒度聚合统计
 */
public class DwsTransOrgTruckModelTransFinishDay {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(args);

        // 并行度设置，部署时应注释，通过 args 指定全局并行度
        env.setParallelism(4);

        // TODO 2. 从 Kafka tms_dwd_trans_trans_finish 主题读取数据
        String topic = "tms_dwd_trans_trans_finish";
        String groupId = "dws_trans_org_truck_model_trans_finish_day";
        KafkaSource<String> kafkaConsumer = KafkaUtil.getKafkaSource(topic, groupId, args);
        SingleOutputStreamOperator<String> source = env
                .fromSource(kafkaConsumer, WatermarkStrategy.noWatermarks(), "kafka_source")
                .uid("kafka_source");

        // TODO 3. 转换数据结构
        SingleOutputStreamOperator<DwsTransOrgTruckModelTransFinishDayBean> mappedStream = source.map(jsonStr -> {
            DwdTransTransFinishBean dwdTransTransFinishBean = JSON.parseObject(jsonStr, DwdTransTransFinishBean.class);
            return DwsTransOrgTruckModelTransFinishDayBean.builder()
                    .orgId(dwdTransTransFinishBean.getStartOrgId())
                    .orgName(dwdTransTransFinishBean.getStartOrgName())
                    .truckId(dwdTransTransFinishBean.getTruckId())
                    .transFinishCountBase(1L)
                    .transFinishDistanceBase(dwdTransTransFinishBean.getActualDistance())
                    .transFinishDurTimeBase(dwdTransTransFinishBean.getTransportTime())
                    .ts(dwdTransTransFinishBean.getTs() + 8 * 60 * 60 * 1000L)
                    .build();
        });

        // TODO 4. 关联获取卡车类别信息
        SingleOutputStreamOperator<DwsTransOrgTruckModelTransFinishDayBean> withTruckModelIdStream = AsyncDataStream.unorderedWait(
                mappedStream,
                new DimAsyncFunction<DwsTransOrgTruckModelTransFinishDayBean>("dim_truck_info") {
                    @Override
                    public void join(DwsTransOrgTruckModelTransFinishDayBean bean, JSONObject dimJsonObj) {
                        bean.setTruckModelId(dimJsonObj.getString("truck_model_id"));
                    }

                    @Override
                    public Tuple2<String,String> getCondition(DwsTransOrgTruckModelTransFinishDayBean bean) {
                        return Tuple2.of("id",bean.getTruckId());
                    }
                },
                60, TimeUnit.SECONDS
        ).uid("with_truck_model_id_stream");

        // TODO 5. 设置水位线
        SingleOutputStreamOperator<DwsTransOrgTruckModelTransFinishDayBean> withWatermarkStream = withTruckModelIdStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<DwsTransOrgTruckModelTransFinishDayBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                        .withTimestampAssigner(new SerializableTimestampAssigner<DwsTransOrgTruckModelTransFinishDayBean>() {
                            @Override
                            public long extractTimestamp(DwsTransOrgTruckModelTransFinishDayBean element, long recordTimestamp) {
                                return element.getTs();
                            }
                        })
        ).uid("watermark_stream");

        // TODO 6. 按照机构 ID 和卡车类型 ID 分组
        KeyedStream<DwsTransOrgTruckModelTransFinishDayBean, String> keyedStream = withWatermarkStream.keyBy(
                bean -> bean.getOrgId() + " : " + bean.getTruckModelId()
        );

        // TODO 7. 开窗
        WindowedStream<DwsTransOrgTruckModelTransFinishDayBean, String, TimeWindow> windowStream =
                keyedStream.window(TumblingEventTimeWindows.of(Time.days(1L)));

        // TODO 8. 引入触发器
        WindowedStream<DwsTransOrgTruckModelTransFinishDayBean, String, TimeWindow> triggerStream =
                windowStream.trigger(new MyTriggerFunction<>());

        // TODO 9. 聚合
        SingleOutputStreamOperator<DwsTransOrgTruckModelTransFinishDayBean> aggregatedStream = triggerStream.aggregate(
                new MyAggregationFunction<DwsTransOrgTruckModelTransFinishDayBean>() {
                    @Override
                    public DwsTransOrgTruckModelTransFinishDayBean add(DwsTransOrgTruckModelTransFinishDayBean value, DwsTransOrgTruckModelTransFinishDayBean accumulator) {
                        if (accumulator == null) {
                            return value;
                        }
                        accumulator.setTransFinishCountBase(
                                accumulator.getTransFinishCountBase() + value.getTransFinishCountBase()
                        );
                        accumulator.setTransFinishDistanceBase(
                                accumulator.getTransFinishDistanceBase().add(value.getTransFinishDistanceBase())
                        );
                        accumulator.setTransFinishDurTimeBase(
                                accumulator.getTransFinishDurTimeBase() + value.getTransFinishDurTimeBase()
                        );
                        return accumulator;
                    }
                },
                new ProcessWindowFunction<DwsTransOrgTruckModelTransFinishDayBean, DwsTransOrgTruckModelTransFinishDayBean, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<DwsTransOrgTruckModelTransFinishDayBean> elements, Collector<DwsTransOrgTruckModelTransFinishDayBean> out) throws Exception {
                        for (DwsTransOrgTruckModelTransFinishDayBean element : elements) {
                            long stt = context.window().getStart();
                            element.setCurDate(DateFormatUtil.toDate(stt - 8 * 60 * 60 * 1000L));
                            element.setTs(System.currentTimeMillis());
                            out.collect(element);
                        }
                    }
                }
        ).uid("aggregate_stream");

        // TODO 10. 关联维度信息
        // 10.1 获取卡车类型名称
        SingleOutputStreamOperator<DwsTransOrgTruckModelTransFinishDayBean> withTruckModelNameStream = AsyncDataStream.unorderedWait(
                aggregatedStream,
                new DimAsyncFunction<DwsTransOrgTruckModelTransFinishDayBean>("dim_truck_model") {
                    @Override
                    public void join(DwsTransOrgTruckModelTransFinishDayBean bean, JSONObject dimJsonObj) {
                        bean.setTruckModelName(dimJsonObj.getString("model_name"));
                    }

                    @Override
                    public Tuple2<String,String> getCondition(DwsTransOrgTruckModelTransFinishDayBean bean) {
                        return Tuple2.of("id",bean.getTruckModelId());
                    }
                },
                60, TimeUnit.SECONDS
        ).uid("with_truck_model_name_stream");

        // 10.2 获取用于关联城市的机构 ID
        SingleOutputStreamOperator<DwsTransOrgTruckModelTransFinishDayBean> withJoinOrgIdStream = AsyncDataStream.unorderedWait(
                withTruckModelNameStream,
                new DimAsyncFunction<DwsTransOrgTruckModelTransFinishDayBean>("dim_base_organ") {
                    @Override
                    public void join(DwsTransOrgTruckModelTransFinishDayBean bean, JSONObject dimJsonObj) {
                        String orgParentId = dimJsonObj.getString("org_parent_id");
                        bean.setJoinOrgId(
                                orgParentId != null ? orgParentId : bean.getOrgId()
                        );
                    }

                    @Override
                    public Tuple2<String,String> getCondition(DwsTransOrgTruckModelTransFinishDayBean bean) {
                        return Tuple2.of("id",bean.getOrgId());
                    }
                },
                60, TimeUnit.SECONDS
        ).uid("with_join_org_id_stream");

        // 10.3 获取城市 ID
        SingleOutputStreamOperator<DwsTransOrgTruckModelTransFinishDayBean> withCityIdStream = AsyncDataStream.unorderedWait(
                withJoinOrgIdStream,
                new DimAsyncFunction<DwsTransOrgTruckModelTransFinishDayBean>("dim_base_organ") {
                    @Override
                    public void join(DwsTransOrgTruckModelTransFinishDayBean bean, JSONObject dimJsonObj){
                        bean.setCityId(dimJsonObj.getString("region_id"));
                    }

                    @Override
                    public Tuple2<String,String> getCondition(DwsTransOrgTruckModelTransFinishDayBean bean) {
                        return Tuple2.of("id",bean.getJoinOrgId());
                    }
                },
                60, TimeUnit.SECONDS
        ).uid("with_city_id_stream");

        // 10.4 获取城市名称
        SingleOutputStreamOperator<DwsTransOrgTruckModelTransFinishDayBean> fullStream = AsyncDataStream.unorderedWait(
                withCityIdStream,
                new DimAsyncFunction<DwsTransOrgTruckModelTransFinishDayBean>("dim_base_region_info") {
                    @Override
                    public void join(DwsTransOrgTruckModelTransFinishDayBean bean, JSONObject dimJsonObj)  {
                        bean.setCityName(dimJsonObj.getString("name"));
                    }

                    @Override
                    public Tuple2<String,String> getCondition(DwsTransOrgTruckModelTransFinishDayBean bean) {
                        return  Tuple2.of("id",bean.getCityId());
                    }
                },
                60, TimeUnit.SECONDS
        ).uid("with_city_name_stream");

        // TODO 11. 写出到 ClickHouse
        fullStream.print(">>>");
        fullStream.addSink(
                ClickHouseUtil.getJdbcSink("insert into dws_trans_org_truck_model_trans_finish_day_base values(?,?,?,?,?,?,?,?,?,?,?)")
        ).uid("clickhouse_stream");

        env.execute();
    }
}