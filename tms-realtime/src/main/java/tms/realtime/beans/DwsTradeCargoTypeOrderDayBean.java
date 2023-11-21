package tms.realtime.beans;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;

/**
 * @author xiaojia
 * @date 2023/11/17 14:39
 * @desc 交易域货物类型下单聚合统计实体类
 */
@Data
@Builder
public class DwsTradeCargoTypeOrderDayBean {
    // 当前日期
    String curDate;

    // 货物类型ID
    String cargoType;

    // 货物类型名称
    String cargoTypeName;

    // 下单金额
    BigDecimal orderAmountBase;


    // 下单次数
    Long orderCountBase;

    // 时间戳
    Long ts;
}