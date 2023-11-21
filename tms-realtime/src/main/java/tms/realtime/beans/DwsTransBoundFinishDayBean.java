package tms.realtime.beans;

/**
 * @author xiaojia
 * @date 2023/11/17 15:21
 * @desc 物流域转运完成实体类
 */
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class DwsTransBoundFinishDayBean {
    // 统计日期
    String curDate;

    // 转运完成次数
    Long boundFinishOrderCountBase;

    // 时间戳
    Long ts;
}