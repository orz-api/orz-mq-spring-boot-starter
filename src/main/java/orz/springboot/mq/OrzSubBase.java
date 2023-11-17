package orz.springboot.mq;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AccessLevel;
import lombok.Getter;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;
import orz.springboot.mq.annotation.OrzSubApi;

import java.lang.reflect.ParameterizedType;
import java.util.Objects;

import static orz.springboot.base.OrzBaseUtils.message;

@Getter(AccessLevel.PROTECTED)
public abstract class OrzSubBase<M, E> {
    private ObjectMapper objectMapper;
    private Class<M> messageType;
    private Converter<String, M> stringMessageConverter;
    private String id;
    private String topic;

    public OrzSubBase() {
    }

    public OrzSubBase(ObjectMapper objectMapper) {
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
        Assert.notNull(annotation, message("@OrzSubApi not annotated", "beanClass", getClass()));
        var attributes = AnnotationUtils.getAnnotationAttributes(annotation);
        Assert.isTrue(attributes.containsKey(OrzSubApi.FIELD_ID), "@OrzSubApi id is missing");
        Assert.isTrue(attributes.containsKey(OrzSubApi.FIELD_TOPIC), "@OrzSubApi topic is missing");
        var id = context.resolveExpressionAsString((String) attributes.get(OrzSubApi.FIELD_ID));
        Assert.notNull(id, message("@OrzSubApi id is null", "beanClass", getClass()));
        var topic = context.resolveExpressionAsString((String) attributes.get(OrzSubApi.FIELD_TOPIC));
        Assert.notNull(topic, message("@OrzSubApi topic is null", "beanClass", getClass()));

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
        return OrzSubConverters.obtainStringConverter(objectMapper, messageType);
    }

    protected abstract void onSubscribe(M message, E extra);
}
