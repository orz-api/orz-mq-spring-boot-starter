package orz.springboot.mq;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AccessLevel;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.FatalBeanException;
import org.springframework.core.annotation.AnnotationUtils;
import orz.springboot.mq.annotation.OrzPubApi;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import static orz.springboot.base.description.OrzDescriptionUtils.desc;

@Getter(AccessLevel.PROTECTED)
public abstract class OrzMqPub<E> {
    protected static final Runnable VOID = () -> {
    };

    private ObjectMapper objectMapper;
    private Class<E> eventType;
    private String topic;

    public OrzMqPub() {
    }

    public OrzMqPub(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public final String getTopic() {
        return Objects.requireNonNull(topic);
    }

    protected void init(OrzMqBeanInitContext context) {
        var annotation = AnnotationUtils.findAnnotation(getClass(), OrzPubApi.class);
        if (annotation == null) {
            throw new FatalBeanException(desc("@OrzPubApi not annotated", "beanClass", getClass()));
        }

        var attributes = AnnotationUtils.getAnnotationAttributes(annotation);
        if (!attributes.containsKey(OrzPubApi.FIELD_TOPIC)) {
            throw new FatalBeanException(desc("@OrzPubApi field missing", "beanClass", getClass(), "field", OrzPubApi.FIELD_TOPIC));
        }

        var topic = context.resolveExpressionAsString((String) attributes.get(OrzPubApi.FIELD_TOPIC));
        if (StringUtils.isBlank(topic)) {
            throw new FatalBeanException(desc("@OrzPubApi topic is blank", "beanClass", getClass()));
        }

        if (this.objectMapper == null) {
            this.objectMapper = context.getApplicationContext().getBean(ObjectMapper.class);
        }
        this.eventType = obtainEventType();
        if (this.eventType == null) {
            throw new FatalBeanException(desc("eventType is null", "beanClass", getClass()));
        }
        this.topic = topic;
    }

    protected Class<E> obtainEventType() {
        return OrzMqUtils.getPubEventType(getClass());
    }

    protected abstract CompletableFuture<Void> publish(E event);
}
