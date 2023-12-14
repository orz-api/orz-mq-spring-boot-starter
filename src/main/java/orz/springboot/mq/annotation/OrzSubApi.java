package orz.springboot.mq.annotation;

import org.springframework.stereotype.Component;

import java.lang.annotation.*;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Component
public @interface OrzSubApi {
    String FIELD_TOPIC = "topic";
    String FIELD_QUALIFIER = "qualifier";

    String topic();

    String qualifier() default "";
}
