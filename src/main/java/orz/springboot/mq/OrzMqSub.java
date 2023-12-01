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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static orz.springboot.base.description.OrzDescriptionUtils.desc;

@Getter(AccessLevel.PROTECTED)
public abstract class OrzMqSub<M, E> {
    private ObjectMapper objectMapper;
    private Class<M> msgType;
    private Converter<String, M> msgStringConverter;
    private String id;
    private String topic;
    private Boolean primary;

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

    public final boolean isPrimary() {
        return Objects.requireNonNull(primary);
    }

    public abstract void start();

    public abstract void stop();

    protected void init(OrzMqBeanInitContext context) {
        var annotation = AnnotationUtils.findAnnotation(getClass(), OrzSubApi.class);
        if (annotation == null) {
            throw new FatalBeanException(desc("@OrzSubApi not annotated", "beanClass", getClass()));
        }

        var attributes = AnnotationUtils.getAnnotationAttributes(annotation);
        if (!attributes.containsKey(OrzSubApi.FIELD_ID)) {
            throw new FatalBeanException(desc("@OrzSubApi field missing", "beanClass", getClass(), "field", OrzSubApi.FIELD_ID));
        }
        if (!attributes.containsKey(OrzSubApi.FIELD_TOPIC)) {
            throw new FatalBeanException(desc("@OrzSubApi field missing", "beanClass", getClass(), "field", OrzSubApi.FIELD_TOPIC));
        }

        var id = (String) attributes.get(OrzSubApi.FIELD_ID);
        if (StringUtils.isBlank(id)) {
            throw new FatalBeanException(desc("@OrzSubApi id is blank", "beanClass", getClass()));
        }
        var expectClassName = Stream.of(id.split("-")).map(StringUtils::capitalize).collect(Collectors.joining()) + "Api";
        if (!expectClassName.equals(getClass().getSimpleName())) {
            throw new FatalBeanException(desc("@OrzSubApi class name is invalid", "beanClass", getClass().getSimpleName(), "expectClassName", expectClassName));
        }
        var topic = context.resolveExpressionAsString((String) attributes.get(OrzSubApi.FIELD_TOPIC));
        if (StringUtils.isBlank(topic)) {
            throw new FatalBeanException(desc("@OrzSubApi topic is blank", "beanClass", getClass()));
        }
        var primary = (Boolean) attributes.getOrDefault(OrzSubApi.FIELD_PRIMARY, true);
        if (primary == null) {
            throw new FatalBeanException(desc("@OrzSubApi primary is null", "beanClass", getClass()));
        }

        if (this.objectMapper == null) {
            this.objectMapper = context.getApplicationContext().getBean(ObjectMapper.class);
        }
        this.msgType = obtainMsgType();
        this.msgStringConverter = obtainMsgStringConverter();
        this.id = id;
        this.topic = topic;
        this.primary = primary;
    }

    protected M convertMsg(String msg) {
        return this.msgStringConverter.convert(msg);
    }

    protected Class<M> obtainMsgType() {
        // noinspection unchecked
        return (Class<M>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
    }

    protected Converter<String, M> obtainMsgStringConverter() {
        return OrzMqSubConverters.obtainStringConverter(objectMapper, msgType);
    }

    protected abstract void subscribe(M msg, E extra);
}
