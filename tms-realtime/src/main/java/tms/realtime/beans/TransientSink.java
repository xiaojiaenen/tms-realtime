package tms.realtime.beans;

/**
 * @author xiaojia
 * @date 2023/11/16 20:21
 * @desc 自定义注解 用于标记不需要向ck中保存的属性
 */
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface TransientSink {
}
