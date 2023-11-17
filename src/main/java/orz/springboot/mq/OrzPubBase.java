package orz.springboot.mq;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AccessLevel;
import lombok.Getter;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.Assert;
import orz.springboot.mq.annotation.OrzPubApi;

import java.lang.reflect.ParameterizedType;
import java.util.concurrent.CompletableFuture;

import static orz.springboot.base.OrzBaseUtils.message;

@Getter(AccessLevel.PROTECTED)
public abstract class OrzPubBase<E> {
    protected static final Runnable VOID = () -> {
    };

    private ObjectMapper objectMapper;
    private Class<E> eventType;

    public OrzPubBase() {
    }

    public OrzPubBase(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    protected void init(OrzMqBeanInitContext context) {
        var annotation = AnnotationUtils.findAnnotation(getClass(), OrzPubApi.class);
        Assert.notNull(annotation, message("@OrzPubApi not annotated", "beanClass", getClass()));

        if (this.objectMapper == null) {
            this.objectMapper = context.getApplicationContext().getBean(ObjectMapper.class);
        }
        this.eventType = obtainEventType();
    }

    protected Class<E> obtainEventType() {
        // noinspection unchecked
        return (Class<E>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
    }

    protected abstract CompletableFuture<Void> onPublish(E event);
}
