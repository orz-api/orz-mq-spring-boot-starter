package orz.springboot.mq.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.util.Assert;
import orz.springboot.mq.OrzMqBeanInitContext;
import orz.springboot.mq.OrzSubBase;
import orz.springboot.mq.kafka.model.OrzSubKafkaRunningChangeE1;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;

import static orz.springboot.base.OrzBaseUtils.message;
import static orz.springboot.mq.kafka.OrzSubKafkaDefinition.RETRY_GROUP_ID_HEADER;
import static orz.springboot.mq.kafka.OrzSubKafkaDefinition.RETRY_TOPIC_POSTFIX;


@Slf4j
@KafkaListener(
        id = "#{__listener.id}",
        groupId = "#{__listener.groupId}",
        topics = {
                "#{__listener.topic}",
                "#{__listener.retryTopic}",
        },
        concurrency = "#{__listener.concurrency}",
        autoStartup = "#{__listener.autoStartup}"
)
public abstract class OrzSubKafka<T> extends OrzSubBase<T, OrzSubKafkaExtra> {
    private ApplicationEventPublisher publisher;
    private KafkaListenerEndpointRegistry registry;
    private String groupId;
    private String retryTopic;
    private Integer concurrency;
    private Boolean autoStartup;

    public OrzSubKafka() {
        super();
    }

    public OrzSubKafka(ObjectMapper objectMapper) {
        super(objectMapper);
    }

    public final String getGroupId() {
        return Objects.requireNonNull(groupId);
    }

    public final String getRetryTopic() {
        return Objects.requireNonNull(retryTopic);
    }

    public final Integer getConcurrency() {
        return concurrency;
    }

    public final Boolean getAutoStartup() {
        return Objects.requireNonNull(autoStartup);
    }

    @KafkaHandler(isDefault = true)
    public void subscribe(ConsumerRecord<String, String> record) {
        if (record.topic().endsWith(RETRY_TOPIC_POSTFIX)) {
            var retryGroupId = Optional.ofNullable(record.headers().lastHeader(RETRY_GROUP_ID_HEADER))
                    .map(header -> new String(header.value(), StandardCharsets.UTF_8))
                    .orElse(null);
            if (!Objects.equals(retryGroupId, getGroupId())) {
                log.info(message("discard retry record", "groupId", getGroupId(), "retryGroupId", retryGroupId));
                return;
            }
        }
        onSubscribe(convertStringMessage(record.value()), new OrzSubKafkaExtra(record));
    }

    @Override
    public void start() {
        var container = registry.getListenerContainer(getId());
        Assert.notNull(container, message("container is null", "id", getId()));
        if (!container.isRunning()) {
            log.info(message("sub start", "id", getId()));
            container.start();
            publisher.publishEvent(new OrzSubKafkaRunningChangeE1(this, true));
        }
    }

    @Override
    public void stop() {
        var container = registry.getListenerContainer(getId());
        Assert.notNull(container, message("container is null", "id", getId()));
        if (container.isRunning()) {
            log.info(message("sub stop", "id", getId()));
            container.stop();
            publisher.publishEvent(new OrzSubKafkaRunningChangeE1(this, false));
        }
    }

    @Override
    protected void init(OrzMqBeanInitContext context) {
        super.init(context);
        var props = context.getApplicationContext().getBean(OrzKafkaProps.class);
        var kafkaProperties = context.getApplicationContext().getBean(KafkaProperties.class);
        var groupPrefix = kafkaProperties.getConsumer().getGroupId();
        if (groupPrefix == null) {
            groupPrefix = context.getApplicationContext().getEnvironment().getProperty("spring.application.name");
        }

        this.publisher = context.getApplicationContext();
        this.registry = context.getApplicationContext().getBean(KafkaListenerEndpointRegistry.class);
        this.groupId = groupPrefix + "." + getId();
        this.retryTopic = getTopic() + RETRY_TOPIC_POSTFIX;
        this.concurrency = Optional.ofNullable(props.getSub().get(getId())).map(OrzKafkaProps.SubConfig::getConcurrency).orElse(kafkaProperties.getListener().getConcurrency());
        this.autoStartup = Optional.ofNullable(props.getSub().get(getId())).map(OrzKafkaProps.SubConfig::isRunning).orElse(kafkaProperties.getListener().isAutoStartup());
    }
}
