package tms.realtime.common;

/**
 * @author xiaojia
 * @date 2023/11/1 21:44
 * @desc 物流实时数仓的常量类
 */
public class TmsConfig {
    public static final String HBASE_ZOOKEEPER_QUORUM="hadoop101,hadoop102,hadoop103";
    public static final String HBASE_NAMESPACE="tms_realtime";

    // clickhouse驱动
    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";
    // clickhouseURL
    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop101:8123/tms_realtime";
}
