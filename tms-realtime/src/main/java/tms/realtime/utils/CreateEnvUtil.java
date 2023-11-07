package tms.realtime.utils;

import com.esotericsoftware.minlog.Log;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.connect.json.DecimalFormat;
import org.apache.kafka.connect.json.JsonConverterConfig;

import java.util.HashMap;
import java.util.Objects;

/**
 * @author xiaojia
 * @date 2023/10/31 20:55
 * @desc 获取执行环境
 */
public class CreateEnvUtil {
    //获取流处理环境
    public static StreamExecutionEnvironment getStreamEnv(String[] args){
        // 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        // 设置并行度
//        env.setParallelism(4);
        // 开启检查点
        env.enableCheckpointing(60000L, CheckpointingMode.EXACTLY_ONCE);
        // 设置检查点的超时时间
        env.getCheckpointConfig().setCheckpointTimeout(120000L);
        // 设置job取消之后，检查点是否保留
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 设置两个检查点之间的最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000L);
        // 设置重启策略
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(1),Time.seconds(3)));
        // 设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://mycluster/tms01/ck");
        // 设置操作hdfs的用户
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String hdfsUserName = parameterTool.get("hadoop-user-name", "root");
        System.setProperty("HADOOP_USER_NAME",hdfsUserName);

        return env;
    }

    /**
     * 获取MySQLSource
     * @param option 区分维度表和事实表
     * @param serverId serverId
     * @param args 参数
     * @return MySqlSource
     */
    public static MySqlSource<String> getMySqlSource(String option,String serverId,String[] args){
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String mysqlHostName = parameterTool.get("mysql-hostname", "hadoop101");
        int mysqlPort = Integer.parseInt(parameterTool.get("mysql-port", "3306"));
        String mysqlUserName = parameterTool.get("mysql-username", "root");
        String mysqlPasswd = parameterTool.get("mysql-passwd", "123456");
        option = parameterTool.get("start-up-options", option);
        serverId = parameterTool.get("server-id", serverId);

        // 创建配置信息 Map 集合，将 Decimal 数据类型的解析格式配置 k-v 置于其中
        HashMap<String, Object> config = new HashMap<>();
        config.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, DecimalFormat.NUMERIC.name());
        // 将前述 Map 集合中的配置信息传递给 JSON 解析 Schema，该 Schema 将用于 MysqlSource 的初始化
        JsonDebeziumDeserializationSchema jsonDebeziumDeserializationSchema =
                new JsonDebeziumDeserializationSchema(false, config);


        MySqlSourceBuilder<String> builder = MySqlSource.<String>builder()
                .hostname(mysqlHostName)
                .port(mysqlPort)
                .username(mysqlUserName)
                .password(mysqlPasswd)
                .deserializer(jsonDebeziumDeserializationSchema);


        switch(option){
            //读取实时数据
            case "dwd":
                String[] dwdTables = new String[]{"tms01.order_info",
                        "tms01.order_cargo",
                        "tms01.transport_task",
                        "tms01.order_org_bound"};
                return builder
                        .databaseList("tms01")
                        .tableList(dwdTables)
                        .startupOptions(StartupOptions.latest())
                        .serverId(serverId)
                        .build();
            //读取维度数据
            case "realtime_dim":
                String[] realtimeDimTables = new String[]{"tms01.user_info",
                        "tms01.user_address",
                        "tms01.base_complex",
                        "tms01.base_dic",
                        "tms01.base_region_info",
                        "tms01.base_organ",
                        "tms01.express_courier",
                        "tms01.express_courier_complex",
                        "tms01.employee_info",
                        "tms01.line_base_shift",
                        "tms01.line_base_info",
                        "tms01.truck_driver",
                        "tms01.truck_info",
                        "tms01.truck_model",
                        "tms01.truck_team"};
                return builder
                        .databaseList("tms01")
                        .tableList(realtimeDimTables)
                        .startupOptions(StartupOptions.initial())
                        .serverId(serverId)
                        .build();
            case "config_dim":
                return builder
                        .databaseList("tms_config")
                        .tableList("tms_config.tms_config_dim")
                        .startupOptions(StartupOptions.initial())
                        .serverId(serverId)
                        .build();
        }
        Log.error("不支持的操作类型");
        return null;
    }
}
