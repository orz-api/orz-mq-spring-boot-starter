package orz.springboot.mq.annotation;

import org.springframework.stereotype.Component;

import java.lang.annotation.*;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Component
public @interface OrzPubApi {
    String FIELD_TOPIC = "topic";

    String topic();
}
