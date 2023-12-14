package orz.springboot.mq;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AccessLevel;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.FatalBeanException;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.convert.converter.Converter;
import orz.springboot.mq.annotation.OrzSubApi;

import java.lang.reflect.ParameterizedType;
import java.util.Objects;

import static orz.springboot.base.description.OrzDescriptionUtils.desc;

@Getter(AccessLevel.PROTECTED)
public abstract class OrzMqSub<M, E> {
    private ObjectMapper objectMapper;
    private Class<M> msgType;
    private Converter<String, M> msgStringConverter;
    private String id;
    private String topic;
    private String qualifier;
    private String fullQualifier;

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

    public final String getQualifier() {
        return Objects.requireNonNull(qualifier);
    }

    public final String getFullQualifier() {
        return Objects.requireNonNull(fullQualifier);
    }

    public abstract void start();

    public abstract void stop();

    protected void init(OrzMqBeanInitContext context) {
        var annotation = AnnotationUtils.findAnnotation(getClass(), OrzSubApi.class);
        if (annotation == null) {
            throw new FatalBeanException(desc("@OrzSubApi not annotated", "beanClass", getClass()));
        }

        var attributes = AnnotationUtils.getAnnotationAttributes(annotation);
        if (!attributes.containsKey(OrzSubApi.FIELD_TOPIC)) {
            throw new FatalBeanException(desc("@OrzSubApi field missing", "beanClass", getClass(), "field", OrzSubApi.FIELD_TOPIC));
        }

        var topic = context.resolveExpressionAsString((String) attributes.get(OrzSubApi.FIELD_TOPIC));
        if (StringUtils.isBlank(topic)) {
            throw new FatalBeanException(desc("@OrzSubApi topic is blank", "beanClass", getClass()));
        }

        var qualifier = context.resolveExpressionAsString((String) attributes.get(OrzSubApi.FIELD_QUALIFIER));
        qualifier = StringUtils.defaultIfBlank(qualifier, "");

        if (this.objectMapper == null) {
            this.objectMapper = context.getApplicationContext().getBean(ObjectMapper.class);
        }
        this.msgType = obtainMsgType(context);
        this.msgStringConverter = obtainMsgStringConverter(context);
        this.id = obtainId(context);
        this.topic = topic;
        this.qualifier = qualifier;
        this.fullQualifier = topic + "@" + qualifier;
    }

    protected M convertMsg(String msg) {
        return this.msgStringConverter.convert(msg);
    }

    protected Class<M> obtainMsgType(OrzMqBeanInitContext context) {
        // noinspection unchecked
        return (Class<M>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
    }

    protected Converter<String, M> obtainMsgStringConverter(OrzMqBeanInitContext context) {
        return OrzMqSubConverters.obtainStringConverter(objectMapper, msgType);
    }

    protected String obtainId(OrzMqBeanInitContext context) {
        return getClass().getSimpleName();
    }

    protected abstract void subscribe(M msg, E extra);
}
