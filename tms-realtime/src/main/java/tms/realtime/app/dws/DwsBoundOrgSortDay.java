package tms.realtime.app.dws;

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
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import tms.realtime.app.func.DimAsyncFunction;
import tms.realtime.app.func.MyAggregationFunction;
import tms.realtime.app.func.MyTriggerFunction;
import tms.realtime.beans.DwdBoundSortBean;
import tms.realtime.beans.DwsBoundOrgSortDayBean;
import tms.realtime.common.TmsConfig;
import tms.realtime.utils.*;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


/**
 * @author xiaojia
 * @date 2023/11/14 15:49
 * @desc 中转域：机构粒度分拣业务过程聚合统计
 */
public class DwsBoundOrgSortDay {
    public static void main(String[] args) throws Exception {
        //1、环境处理
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(args);

        // 并行度设置，部署时应注释，通过 args 指定全局并行度
        env.setParallelism(1);
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
                                .ts(dwdBoundSortBean.getTs() + 8 * 60 * 60 * 1000L)
                                .build();
                        return sortDayBean;
                    }
                });
//        boundOrgSortDayBeanDS.print(">>>");
        //4、指定Watermark以及提取事件时间字段
        SingleOutputStreamOperator<DwsBoundOrgSortDayBean> withWatermarkDS = boundOrgSortDayBeanDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<DwsBoundOrgSortDayBean>forBoundedOutOfOrderness(Duration.ofSeconds(10L))
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
        WindowedStream<DwsBoundOrgSortDayBean, String, TimeWindow> triggerDS
                = windowDS.trigger(new MyTriggerFunction<DwsBoundOrgSortDayBean>());
        //8、聚合
        SingleOutputStreamOperator<DwsBoundOrgSortDayBean> aggregateDS
                = triggerDS.aggregate(new MyAggregationFunction<DwsBoundOrgSortDayBean>() {
                                          @Override
                                          public DwsBoundOrgSortDayBean add(DwsBoundOrgSortDayBean value, DwsBoundOrgSortDayBean acc) {
                                              if (acc == null) {
                                                  return value;
                                              }
                                              acc.setSortCountBase(value.getSortCountBase() + 1);
                                              return acc;
                                          }
                                      },
                        new ProcessWindowFunction<DwsBoundOrgSortDayBean, DwsBoundOrgSortDayBean, String, TimeWindow>() {
                            @Override
                            public void process(String key, Context context, Iterable<DwsBoundOrgSortDayBean> elements, Collector<DwsBoundOrgSortDayBean> out) throws Exception {
                                for (DwsBoundOrgSortDayBean element : elements) {
                                    // 将窗口起始时间格式化为 yyyy-mm-dd HH:mm:ss 格式的日期字符串
                                    // 左移八小时
                                    long stt = context.window().getStart();
                                    // 补充日期字段，修改时间戳字段，并发送到下游
                                    // 补充curDate字段
                                    element.setCurDate(DateFormatUtil.toDate(stt - 8 * 60 * 60 * 1000L));
                                    // 将时间戳置为系统时间
                                    element.setTs(System.currentTimeMillis());
                                    out.collect(element);
                                }
                            }
                        }
                )
                .uid("aggregate_ds");
        aggregateDS.print(">>>>");
        //9、关联维度（城市、省份）
        //关联机构维度，获取机构名称
//        SingleOutputStreamOperator<DwsBoundOrgSortDayBean> withOrgNameDS = aggregateDS.map(new MapFunction<DwsBoundOrgSortDayBean, DwsBoundOrgSortDayBean>() {
//            @Override
//            public DwsBoundOrgSortDayBean map(DwsBoundOrgSortDayBean dwsBoundOrgSortDayBean) throws Exception {
//                //根据流中的对象获取要关联的维度主键
//                String orgId = dwsBoundOrgSortDayBean.getOrgId();
//                //根据维度的主键到维度表中获取对应的维度对象
//                JSONObject dimJsonObj = DimUtil.getDimInfo(TmsConfig.HBASE_NAMESPACE, "dim_base_organ", Tuple2.of("id", orgId));
//                //将维度对象的属性补充到流中对象上
//                dwsBoundOrgSortDayBean.setOrgName(dimJsonObj.getString("org_name"));
//                return dwsBoundOrgSortDayBean;
//            }
//        });

        SingleOutputStreamOperator<DwsBoundOrgSortDayBean> withOrgNameStream = AsyncDataStream.unorderedWait(
                aggregateDS,
                new DimAsyncFunction<DwsBoundOrgSortDayBean>("dim_base_organ") {
                    @Override
                    public void join(DwsBoundOrgSortDayBean bean, JSONObject dimJsonObj) {
                        // 获取上级机构 ID
                        String orgParentId = dimJsonObj.getString("org_parent_id");
                        bean.setOrgName(dimJsonObj.getString("org_name"));
                        bean.setJoinOrgId(orgParentId != null ? orgParentId : bean.getOrgId());
                    }

                    @Override
                    public Tuple2<String,String> getCondition(DwsBoundOrgSortDayBean bean) {
                        return Tuple2.of("id",bean.getOrgId());
                    }
                },
                60, TimeUnit.SECONDS
        ).uid("with_org_name_stream");

        // 9.2 获取城市 ID
        SingleOutputStreamOperator<DwsBoundOrgSortDayBean> withCityIdStream = AsyncDataStream.unorderedWait(
                withOrgNameStream,
                new DimAsyncFunction<DwsBoundOrgSortDayBean>("dim_base_organ") {
                    @Override
                    public void join(DwsBoundOrgSortDayBean bean, JSONObject dimJsonObj) {
                        bean.setCityId(dimJsonObj.getString("region_id"));
                    }

                    @Override
                    public Tuple2<String,String> getCondition(DwsBoundOrgSortDayBean bean) {
                        return Tuple2.of("id",bean.getJoinOrgId());
                    }
                },
                60, TimeUnit.SECONDS
        ).uid("with_city_id_stream");

        // 9.3 获取城市名称及省份 ID
        SingleOutputStreamOperator<DwsBoundOrgSortDayBean> withCityNameStream = AsyncDataStream.unorderedWait(
                withCityIdStream,
                new DimAsyncFunction<DwsBoundOrgSortDayBean>("dim_base_region_info") {
                    @Override
                    public void join(DwsBoundOrgSortDayBean bean, JSONObject dimJsonObj)  {
                        bean.setCityName(dimJsonObj.getString("name"));
                        bean.setProvinceId(dimJsonObj.getString("parent_id"));
                    }

                    @Override
                    public Tuple2<String,String> getCondition(DwsBoundOrgSortDayBean bean) {
                        return Tuple2.of("id",bean.getCityId());
                    }
                },
                60, TimeUnit.SECONDS
        ).uid("with_city_name_stream");

        // 9.4 获取省份名称
        SingleOutputStreamOperator<DwsBoundOrgSortDayBean> fullStream = AsyncDataStream.unorderedWait(
                withCityNameStream,
                new DimAsyncFunction<DwsBoundOrgSortDayBean>("dim_base_region_info") {
                    @Override
                    public void join(DwsBoundOrgSortDayBean bean, JSONObject dimJsonObj)  {
                        bean.setProvinceName(dimJsonObj.getString("name"));
                    }

                    @Override
                    public Tuple2<String,String> getCondition(DwsBoundOrgSortDayBean bean) {
                        return Tuple2.of("id",bean.getProvinceId());
                    }
                },
                60, TimeUnit.SECONDS
        ).uid("with_province_name_stream");

        // TODO 10. 写出到 ClickHouse
        fullStream.print(">>>>");
        fullStream.addSink(
                ClickHouseUtil.getJdbcSink("insert into dws_bound_org_sort_day_base values(?,?,?,?,?,?,?,?,?)")
        ).uid("clickhouse_sink");

        env.execute();
    }
}
