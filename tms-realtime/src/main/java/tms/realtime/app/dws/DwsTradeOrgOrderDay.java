package tms.realtime.app.dws;

/**
 * @author xiaojia
 * @date 2023/11/17 14:52
 * @desc 交易域机构粒度下单统计
 */
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
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
import tms.realtime.app.func.DimAsyncFunction;
import tms.realtime.app.func.MyAggregationFunction;
import tms.realtime.app.func.MyTriggerFunction;
import tms.realtime.beans.DwdTradeOrderDetailBean;
import tms.realtime.beans.DwsTradeOrgOrderDayBean;
import tms.realtime.utils.ClickHouseUtil;
import tms.realtime.utils.CreateEnvUtil;
import tms.realtime.utils.DateFormatUtil;
import tms.realtime.utils.KafkaUtil;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class DwsTradeOrgOrderDay {
    public static void main(String[] args) throws Exception {
        // TODO 1. 初始化表处理环境
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(args);

        // 并行度设置，部署时应注释，通过 args 指定全局并行度
        env.setParallelism(4);

        // TODO 2. 从 Kafka tms_dwd_trade_order_detail 主题读取数据
        String topic = "tms_dwd_trade_order_detail";
        String groupId = "dws_trade_org_order_day";

        KafkaSource<String> kafkaConsumer = KafkaUtil.getKafkaSource(topic, groupId, args);
        SingleOutputStreamOperator<String> source = env
                .fromSource(kafkaConsumer, WatermarkStrategy.noWatermarks(), "kafka_source")
                .uid("kafka_source");

        // TODO 3. 转换数据格式
        SingleOutputStreamOperator<DwsTradeOrgOrderDayBean> mappedStream =
                source.map(new MapFunction<String, DwsTradeOrgOrderDayBean>() {
                    @Override
                    public DwsTradeOrgOrderDayBean map(String jsonStr) {
                        DwdTradeOrderDetailBean dwdTradeOrderDetailBean = JSON.parseObject(jsonStr, DwdTradeOrderDetailBean.class);
                        return DwsTradeOrgOrderDayBean.builder()
                                .senderDistrictId(dwdTradeOrderDetailBean.getSenderDistrictId())
                                .cityId(dwdTradeOrderDetailBean.getSenderCityId())
                                .orderAmountBase(dwdTradeOrderDetailBean.getAmount())
                                .orderCountBase(1L)
                                // 右移八小时
                                .ts(dwdTradeOrderDetailBean.getTs() + 8 * 60 * 60 * 1000L)
                                .build();
                    }
                });

        // TODO 4. 关联机构信息
        SingleOutputStreamOperator<DwsTradeOrgOrderDayBean> withOrgIdStream = AsyncDataStream.unorderedWait(
                mappedStream,
                new DimAsyncFunction<DwsTradeOrgOrderDayBean>("dim_base_organ") {
                    @Override
                    public void join(DwsTradeOrgOrderDayBean bean, JSONObject dimJsonObj) {
                        bean.setOrgId(dimJsonObj.getString("id"));
                        bean.setOrgName(dimJsonObj.getString("org_name"));
                    }

                    @Override
                    public Tuple2<String, String> getCondition(DwsTradeOrgOrderDayBean bean) {
                        return Tuple2.of("region_id", bean.getSenderDistrictId());
                    }
                }, 5 * 60,
                TimeUnit.SECONDS
        ).uid("with_org_info_stream");

        // TODO 5. 设置水位线
        SingleOutputStreamOperator<DwsTradeOrgOrderDayBean> withWatermarkStream = withOrgIdStream.assignTimestampsAndWatermarks(WatermarkStrategy
                .<DwsTradeOrgOrderDayBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                .withTimestampAssigner(
                        new SerializableTimestampAssigner<DwsTradeOrgOrderDayBean>() {
                            @Override
                            public long extractTimestamp(DwsTradeOrgOrderDayBean element, long recordTimestamp) {
                                return element.getTs();
                            }
                        }
                )).uid("watermark_stream");

        // TODO 6. 按照机构ID分组
        KeyedStream<DwsTradeOrgOrderDayBean, String> keyedStream =
                withWatermarkStream.keyBy(DwsTradeOrgOrderDayBean::getOrgId);

        // TODO 7. 开窗
        WindowedStream<DwsTradeOrgOrderDayBean, String, TimeWindow> windowStream =
                keyedStream.window(TumblingEventTimeWindows.of(Time.days(1L)));

        // TODO 8. 引入触发器
        WindowedStream<DwsTradeOrgOrderDayBean, String, TimeWindow> triggeredStream = windowStream.trigger(
                new MyTriggerFunction<DwsTradeOrgOrderDayBean>()
        );

        // TODO 9. 聚合
        SingleOutputStreamOperator<DwsTradeOrgOrderDayBean> aggregatedStream = triggeredStream.aggregate(
                new MyAggregationFunction<DwsTradeOrgOrderDayBean>() {
                    @Override
                    public DwsTradeOrgOrderDayBean add(DwsTradeOrgOrderDayBean value, DwsTradeOrgOrderDayBean accumulator) {
                        if (accumulator == null) {
                            return value;
                        }
                        accumulator.setOrderCountBase(
                                value.getOrderCountBase() + accumulator.getOrderCountBase()
                        );
                        accumulator.setOrderAmountBase(
                                value.getOrderAmountBase().add(accumulator.getOrderAmountBase())
                        );
                        return accumulator;
                    }
                },
                new ProcessWindowFunction<DwsTradeOrgOrderDayBean, DwsTradeOrgOrderDayBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<DwsTradeOrgOrderDayBean> elements, Collector<DwsTradeOrgOrderDayBean> out) throws Exception {
                        // 将窗口起始时间格式化为 yyyy-mm-dd HH:mm:ss 格式的日期字符串
                        // 左移八小时
                        long stt = context.window().getStart() - 8 * 60 * 60 * 1000L;
                        String curDate = DateFormatUtil.toDate(stt);

                        // 补充日期字段，修改时间戳字段，并发送到下游
                        for (DwsTradeOrgOrderDayBean element : elements) {
                            // 补充curDate字段
                            element.setCurDate(curDate);
                            // 将时间戳置为系统时间
                            element.setTs(System.currentTimeMillis());
                            out.collect(element);
                        }
                    }
                }
        ).uid("aggregate_stream");

        // TODO 10. 补充维度信息
        SingleOutputStreamOperator<DwsTradeOrgOrderDayBean> fullStream = AsyncDataStream.unorderedWait(
                aggregatedStream,
                new DimAsyncFunction<DwsTradeOrgOrderDayBean>("dim_base_region_info") {
                    @Override
                    public void join(DwsTradeOrgOrderDayBean bean, JSONObject dimJsonObj)  {
                        bean.setCityName(dimJsonObj.getString("name"));
                    }

                    @Override
                    public Tuple2<String,String> getCondition(DwsTradeOrgOrderDayBean bean) {
                        return Tuple2.of("id",bean.getCityId());
                    }
                },
                5 * 60,
                TimeUnit.SECONDS
        ).uid("with_city_name_stream");

        // TODO 11. 写出到 ClickHouse
        fullStream.print(">>>>");
        fullStream.addSink(ClickHouseUtil.<DwsTradeOrgOrderDayBean>getJdbcSink(
                        "insert into dws_trade_org_order_day_base values(?,?,?,?,?,?,?,?)"))
                .uid("clickhouse_stream");

        env.execute();
    }

}