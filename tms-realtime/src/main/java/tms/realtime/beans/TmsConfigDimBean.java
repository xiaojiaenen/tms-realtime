package tms.realtime.beans;

import lombok.Data;

/**
 * @author xiaojia
 * @date 2023/11/2 10:02
 * @desc 实体类
 */

@Data
public class TmsConfigDimBean {
    // 数据源表表名
    String sourceTable;

    // 目标表表名
    String sinkTable;

    // 目标表表名
    String sinkFamily;

    // 需要的字段列表
    String sinkColumns;

    // 需要的主键
    String sinkPk;
}
