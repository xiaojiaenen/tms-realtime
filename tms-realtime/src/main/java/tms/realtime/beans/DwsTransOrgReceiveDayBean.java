package tms.realtime.beans;

/**
 * @author xiaojia
 * @date 2023/11/17 15:31
 * @desc 物流域机构粒度揽收统计实体类
 */
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class DwsTransOrgReceiveDayBean {

    // 统计日期
    String curDate;

    // 转运站ID
    String orgId;

    // 转运站名称
    String orgName;

    // 地区ID
    @TransientSink
    String districtId;

    // 城市ID
    String cityId;

    // 城市名称
    String cityName;

    // 省份ID
    String provinceId;

    // 省份名称
    String provinceName;

    // 揽收次数（一个订单算一次）
    Long receiveOrderCountBase;

    // 时间戳
    Long ts;
}