package orz.springboot.mq;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AccessLevel;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.convert.converter.Converter;
import orz.springboot.mq.annotation.OrzSubApi;

import java.lang.reflect.ParameterizedType;
import java.util.Objects;

import static orz.springboot.base.OrzBaseUtils.message;

@Getter(AccessLevel.PROTECTED)
public abstract class OrzMqSub<M, E> {
    private ObjectMapper objectMapper;
    private Class<M> messageType;
    private Converter<String, M> stringMessageConverter;
    private String id;
    private String topic;

    public OrzMqSub() {
    }

    public OrzMqSub(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public final String getId() {
        return Objects.requireNonNull(id);
    }

    public final String getTopic() {
        return Objects.requireNonNull(topic);
    }

    public abstract void start();

    public abstract void stop();

    protected void init(OrzMqBeanInitContext context) {
        var annotation = AnnotationUtils.findAnnotation(getClass(), OrzSubApi.class);
        if (annotation == null) {
            throw new RuntimeException(message("@OrzSubApi not annotated", "beanClass", getClass()));
        }

        var attributes = AnnotationUtils.getAnnotationAttributes(annotation);
        if (!attributes.containsKey(OrzSubApi.FIELD_ID)) {
            throw new RuntimeException(message("@OrzSubApi field missing", "beanClass", getClass(), "field", OrzSubApi.FIELD_ID));
        }
        if (!attributes.containsKey(OrzSubApi.FIELD_TOPIC)) {
            throw new RuntimeException(message("@OrzSubApi field missing", "beanClass", getClass(), "field", OrzSubApi.FIELD_TOPIC));
        }

        var id = context.resolveExpressionAsString((String) attributes.get(OrzSubApi.FIELD_ID));
        if (StringUtils.isBlank(id)) {
            throw new RuntimeException(message("@OrzSubApi id is blank", "beanClass", getClass()));
        }
        var topic = context.resolveExpressionAsString((String) attributes.get(OrzSubApi.FIELD_TOPIC));
        if (StringUtils.isBlank(topic)) {
            throw new RuntimeException(message("@OrzSubApi topic is blank", "beanClass", getClass()));
        }

        if (this.objectMapper == null) {
            this.objectMapper = context.getApplicationContext().getBean(ObjectMapper.class);
        }
        this.messageType = obtainMessageType();
        this.stringMessageConverter = obtainStringMessageConverter();
        this.id = id;
        this.topic = topic;
    }

    protected M convertStringMessage(String message) {
        return this.stringMessageConverter.convert(message);
    }

    protected Class<M> obtainMessageType() {
        // noinspection unchecked
        return (Class<M>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
    }

    protected Converter<String, M> obtainStringMessageConverter() {
        return OrzMqSubConverters.obtainStringConverter(objectMapper, messageType);
    }

    protected abstract void onSubscribe(M message, E extra);
}
