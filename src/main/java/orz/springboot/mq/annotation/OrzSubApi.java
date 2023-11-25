package orz.springboot.mq.annotation;

import org.springframework.stereotype.Component;
import orz.springboot.base.annotation.OrzFullyQualifier;

import java.lang.annotation.*;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Component
@OrzFullyQualifier
public @interface OrzSubApi {
    String FIELD_ID = "id";
    String FIELD_TOPIC = "topic";

    String id();

    String topic();
}
