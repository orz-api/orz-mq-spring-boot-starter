package orz.springboot.mq.annotation;

import org.springframework.stereotype.Component;

import java.lang.annotation.*;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Component
public @interface OrzSubApi {
    String FIELD_ID = "id";
    String FIELD_TOPIC = "topic";
    String FIELD_PRIMARY = "primary";

    String id();

    String topic();

    boolean primary() default true;
}
