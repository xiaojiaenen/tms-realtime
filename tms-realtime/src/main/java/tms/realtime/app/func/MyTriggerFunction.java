package tms.realtime.app.func;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @author xiaojia
 * @date 2023/11/14 21:05
 * @desc 自定义触发器 每10s触发一次窗口计算
 */
public class MyTriggerFunction<T> extends Trigger<T, TimeWindow> {
    @Override
    public TriggerResult onElement(T element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        ValueStateDescriptor<Boolean> valueStateDescriptor
                = new ValueStateDescriptor<Boolean>("isFirstState",Boolean.class);
        ValueState<Boolean> isFirstState = ctx.getPartitionedState(valueStateDescriptor);
        Boolean isFirst = isFirstState.value();
        if(isFirst == null){
            //如果是窗口中的第一个元素   注册定时器，将事件时间向下取整后，注册10s后执行的定时器
            isFirstState.update(true);
            long nextTime = timestamp - timestamp % 10000L + 10000L;
            ctx.registerEventTimeTimer(nextTime);
        }else  if(isFirst){
            isFirstState.update(false);
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
//        System.out.println("触发器onEventTime");
        long end = window.getEnd();
        if(time < end){
//            System.out.println("触发器onEventTime~~~");
            if(time + 10000L < end){
                ctx.registerEventTimeTimer(time + 10000L);
//                System.out.println("Registered timer for window end time: " + (time + 10000L));
            }
            return TriggerResult.FIRE;

        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
//        System.out.println("触发器clear");
        ctx.deleteEventTimeTimer(window.maxTimestamp());
    }
}
