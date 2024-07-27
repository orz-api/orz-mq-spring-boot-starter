package orz.springboot.mq;

import lombok.AccessLevel;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.FatalBeanException;
import org.springframework.core.annotation.AnnotationUtils;
import orz.springboot.mq.annotation.OrzSubApi;

import java.util.Objects;

import static orz.springboot.base.description.OrzDescriptionUtils.desc;

@Getter(AccessLevel.PROTECTED)
public abstract class OrzMqSub<M, E> {
    private String id;
    private String topic;
    private String qualifier;
    private String fullQualifier;
    private Class<M> messageType;

    public OrzMqSub() {
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

        this.id = obtainId(context);
        if (StringUtils.isBlank(id)) {
            throw new FatalBeanException(desc("id is blank", "beanClass", getClass()));
        }
        this.topic = topic;
        this.qualifier = qualifier;
        if (StringUtils.isNotBlank(qualifier)) {
            this.fullQualifier = topic + "@" + qualifier;
        } else {
            this.fullQualifier = topic;
        }

        this.messageType = obtainMessageType();
        if (this.messageType == null) {
            throw new FatalBeanException(desc("messageType is null", "beanClass", getClass()));
        }
    }

    protected String obtainId(OrzMqBeanInitContext context) {
        return getClass().getSimpleName();
    }

    protected Class<M> obtainMessageType() {
        return OrzMqUtils.getSubMessageType(getClass());
    }

    protected abstract void subscribe(M message, E extra);
}
