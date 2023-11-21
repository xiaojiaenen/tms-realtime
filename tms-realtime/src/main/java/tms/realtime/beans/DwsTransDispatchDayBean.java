package tms.realtime.beans;

/**
 * @author xiaojia
 * @date 2023/11/17 15:23
 * @desc 物流域发单统计实体类
 */
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class DwsTransDispatchDayBean {
    // 统计日期
    String curDate;

    // 发单数
    Long dispatchOrderCountBase;

    // 时间戳
    Long ts;
}