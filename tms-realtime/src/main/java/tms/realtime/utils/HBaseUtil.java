package tms.realtime.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
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

}
