package tms.realtime.utils;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import tms.realtime.common.TmsConfig;

import java.io.IOException;

/**
 * @author xiaojia
 * @date 2023/11/1 21:38
 * @desc 操作HBase工具类
 */
public class HBaseUtil {
    private static Connection conn;

    static {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", TmsConfig.HBASE_ZOOKEEPER_QUORUM);
        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    //创建表
    public static void createTable(String nameSpace, String tableName, String... families) {

        Admin admin = null;
        try {
            if (families.length < 1) {
                System.out.println("至少需要一个列族");
                return;
            }
            admin = conn.getAdmin();
            if (admin.tableExists(TableName.valueOf(nameSpace, tableName))) {
                System.out.println(nameSpace + ":" + tableName + "已存在");
                return;
            }
            TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(TableName.valueOf(nameSpace, tableName));
            for (String family :
                    families) {
                ColumnFamilyDescriptor familyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family)).build();
                builder.setColumnFamily(familyDescriptor);
            }
            admin.createTable(builder.build());
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (admin != null) {
                try {
                    admin.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    //向HBase中插入一行数据
    public static void putRow(String nameSpace, String tableName, Put put) {
        BufferedMutator bufferedMutator = null;
        try {
            BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(nameSpace, tableName));
            params.writeBufferSize(5 * 1024 * 1024);
            params.setWriteBufferPeriodicFlushTimeoutMs(3000L);
            bufferedMutator = conn.getBufferedMutator(params);
            bufferedMutator.mutate(put);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (bufferedMutator != null) {
                try {
                    bufferedMutator.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    /**
     * 根据主键从hbase中查询一行数据
     *
     * @param namespace        命名空间
     * @param tableName        表名
     * @param rowKeyNameAndKey 主键名称和值
     * @return json对象
     */
    public static JSONObject getRowByPrimaryKey(String namespace, String tableName, Tuple2<String, String> rowKeyNameAndKey) {
        Table table = null;
        JSONObject dimJsonObj = null;
        String rowKeyName = rowKeyNameAndKey.f0;
        String rowKeyValue = rowKeyNameAndKey.f1;

        try {
            table = conn.getTable(TableName.valueOf(namespace, tableName));
            Get get = new Get(Bytes.toBytes(rowKeyValue));
            Result result = table.get(get);
            Cell[] cells = result.rawCells();
            if (cells.length > 0) {
                dimJsonObj = new JSONObject();
                dimJsonObj.put(rowKeyName, rowKeyValue);
                for (Cell cell : cells) {
                    dimJsonObj.put(Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
                }
            } else {
                System.out.println("从hbase表中没有找到对应维度数据");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return dimJsonObj;
    }

    /**
     * 根据外键从hbase中查询一行数据
     *
     * @param namespace            命名空间
     * @param tableName            表名
     * @param foreignKeyNameAndKey 主键名称和值
     * @return json对象
     */
    public static JSONObject getRowByForeignKey(String namespace, String tableName, Tuple2<String, String> foreignKeyNameAndKey) {
        Table table = null;
        JSONObject dimJsonObj = null;
        String foreignKeyName = foreignKeyNameAndKey.f0;
        String foreignKeyValue = foreignKeyNameAndKey.f1;

        try {
            table = conn.getTable(TableName.valueOf(namespace, tableName));
            Scan scan = new Scan();
            SingleColumnValueFilter filter
                    = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes(foreignKeyName)
                    , CompareOperator.EQUAL, Bytes.toBytes(foreignKeyValue));
            filter.setFilterIfMissing(true);
            scan.setFilter(filter);
            ResultScanner scanner = table.getScanner(scan);
            Result result = scanner.next();
            if (result != null) {
                Cell[] cells = result.rawCells();
                if (cells.length > 0) {
                    dimJsonObj = new JSONObject();
                    dimJsonObj.put("id", Bytes.toString(result.getRow()));
                    for (Cell cell : cells) {
                        dimJsonObj.put(Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
                    }
                }
            } else {
                System.out.println("从hbase表中没有找到对应维度数据");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return dimJsonObj;
    }

}
