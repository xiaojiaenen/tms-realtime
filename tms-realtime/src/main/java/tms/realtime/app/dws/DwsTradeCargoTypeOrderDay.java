package tms.realtime.app.dws;

/**
 * @author xiaojia
 * @date 2023/11/17 14:42
 * @desc 交易域货物类型下单聚合统计
 */
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
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
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import tms.realtime.app.func.DimAsyncFunction;
import tms.realtime.app.func.MyAggregationFunction;
import tms.realtime.app.func.MyTriggerFunction;
import tms.realtime.beans.DwdTradeOrderDetailBean;
import tms.realtime.beans.DwsTradeCargoTypeOrderDayBean;
import tms.realtime.utils.ClickHouseUtil;
import tms.realtime.utils.CreateEnvUtil;
import tms.realtime.utils.DateFormatUtil;
import tms.realtime.utils.KafkaUtil;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class DwsTradeCargoTypeOrderDay {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(args);

        // 并行度设置，部署时应注释，通过 args 指定全局并行度
        env.setParallelism(4);

        // TODO 2. 从 Kafka 指定主题消费数据
        String topic = "tms_dwd_trade_order_detail";
        String groupId = "dws_trade_cargo_type_order_day";
        KafkaSource<String> kafkaConsumer = KafkaUtil.getKafkaSource(topic, groupId, args);
        SingleOutputStreamOperator<String> source = env
                .fromSource(kafkaConsumer, WatermarkStrategy.noWatermarks(), "kafka_source")
                .uid("kafka_source");

        // TODO 3. 转换数据结构
        SingleOutputStreamOperator<DwsTradeCargoTypeOrderDayBean> mappedStream = source.map(jsonStr -> {
            DwdTradeOrderDetailBean bean = JSON.parseObject(jsonStr, DwdTradeOrderDetailBean.class);
            return DwsTradeCargoTypeOrderDayBean.builder()
                    .cargoType(bean.getCargoType())
                    .orderAmountBase(bean.getAmount())
                    .orderCountBase(1L)
                    .ts(bean.getTs() + 8 * 60 * 60 * 1000L)
                    .build();
        });

        // TODO 4. 设置水位线
        SingleOutputStreamOperator<DwsTradeCargoTypeOrderDayBean> withWatermarkStream = mappedStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<DwsTradeCargoTypeOrderDayBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                        .withTimestampAssigner(new SerializableTimestampAssigner<DwsTradeCargoTypeOrderDayBean>() {
                            @Override
                            public long extractTimestamp(DwsTradeCargoTypeOrderDayBean bean, long recordTimestamp) {
                                return bean.getTs();
                            }
                        })

        ).uid("watermark_stream");

        // TODO 5. 按照货物类别分组
        KeyedStream<DwsTradeCargoTypeOrderDayBean, String> keyedByCargoTypeStream = withWatermarkStream.keyBy(DwsTradeCargoTypeOrderDayBean::getCargoType);

        // TODO 6. 开窗
        WindowedStream<DwsTradeCargoTypeOrderDayBean, String, TimeWindow> windowStream = keyedByCargoTypeStream
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.days(1L)));

        // TODO 7. 引入触发器
        WindowedStream<DwsTradeCargoTypeOrderDayBean, String, TimeWindow> withTriggerStream = windowStream.trigger(
                new MyTriggerFunction<DwsTradeCargoTypeOrderDayBean>()
        );

        // TODO 8. 聚合
        SingleOutputStreamOperator<DwsTradeCargoTypeOrderDayBean> aggregatedStream = withTriggerStream.aggregate(
                new MyAggregationFunction<DwsTradeCargoTypeOrderDayBean>() {
                    @Override
                    public DwsTradeCargoTypeOrderDayBean add(DwsTradeCargoTypeOrderDayBean value, DwsTradeCargoTypeOrderDayBean accumulator) {
                        if (accumulator == null) {
                            return value;
                        }
                        accumulator.setOrderAmountBase(
                                value.getOrderAmountBase().add(accumulator.getOrderAmountBase()));
                        accumulator.setOrderCountBase(
                                value.getOrderCountBase() + accumulator.getOrderCountBase());
                        return accumulator;
                    }
                },
                new ProcessWindowFunction<DwsTradeCargoTypeOrderDayBean, DwsTradeCargoTypeOrderDayBean, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<DwsTradeCargoTypeOrderDayBean> elements, Collector<DwsTradeCargoTypeOrderDayBean> out) throws Exception {
                        long stt = context.window().getStart() - 8 * 60 * 60 * 1000L;
                        String curDate = DateFormatUtil.toDate(stt);
                        for (DwsTradeCargoTypeOrderDayBean element : elements) {
                            element.setCurDate(curDate);
                            element.setTs(System.currentTimeMillis());
                            out.collect(element);
                        }
                    }
                }
        ).uid("aggregate_stream");

        // TODO 9. 补充货物类型字段
        SingleOutputStreamOperator<DwsTradeCargoTypeOrderDayBean> fullStream = AsyncDataStream.unorderedWait(aggregatedStream,
                new DimAsyncFunction<DwsTradeCargoTypeOrderDayBean>("dim_base_dic") {
                    @Override
                    public void join(DwsTradeCargoTypeOrderDayBean bean, JSONObject dimJsonObj)  {
                        bean.setCargoTypeName(dimJsonObj.getString("name"));
                    }

                    @Override
                    public Tuple2<String, String> getCondition(DwsTradeCargoTypeOrderDayBean bean) {
                        return  Tuple2.of("id",bean.getCargoType());
                    }
                }, 5 * 60L,
                TimeUnit.SECONDS).uid("with_cargo_type_name_stream");

        // TODO 10. 写入 ClickHouse
        fullStream.addSink(ClickHouseUtil.getJdbcSink(
                "insert into dws_trade_cargo_type_order_day_base values(?,?,?,?,?,?)"
        )).uid("clickhouse_sink");

        env.execute();
    }
}