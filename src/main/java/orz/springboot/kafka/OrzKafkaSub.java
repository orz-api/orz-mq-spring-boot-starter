package orz.springboot.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import orz.springboot.kafka.model.OrzKafkaSubRunningChangeE1;
import orz.springboot.mq.OrzMqBeanInitContext;
import orz.springboot.mq.OrzMqSub;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;

import static orz.springboot.base.OrzBaseUtils.message;
import static orz.springboot.kafka.OrzKafkaDefinition.RETRY_GROUP_ID_HEADER;
import static orz.springboot.kafka.OrzKafkaDefinition.RETRY_TOPIC_POSTFIX;


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
public abstract class OrzKafkaSub<T> extends OrzMqSub<T, OrzKafkaSubExtra> {
    private ApplicationEventPublisher publisher;
    private KafkaListenerEndpointRegistry registry;
    private String groupId;
    private String retryTopic;
    private Integer concurrency;
    private Boolean autoStartup;

    public OrzKafkaSub() {
        super();
    }

    public OrzKafkaSub(ObjectMapper objectMapper) {
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
        subscribe(convertStringMessage(record.value()), new OrzKafkaSubExtra(record));
    }

    @Override
    public void start() {
        var container = registry.getListenerContainer(getId());
        if (container == null) {
            throw new RuntimeException(message("listener container is null", "id", getId()));
        }
        if (!container.isRunning()) {
            log.info(message("kafka sub start", "id", getId()));
            container.start();
            publisher.publishEvent(new OrzKafkaSubRunningChangeE1(this, true));
        }
    }

    @Override
    public void stop() {
        var container = registry.getListenerContainer(getId());
        if (container == null) {
            throw new RuntimeException(message("listener container is null", "id", getId()));
        }
        if (container.isRunning()) {
            log.info(message("kafka sub stop", "id", getId()));
            container.stop();
            publisher.publishEvent(new OrzKafkaSubRunningChangeE1(this, false));
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
        if (StringUtils.isBlank(groupPrefix)) {
            throw new RuntimeException(message("kafka consumer group prefix is blank"));
        }

        this.publisher = context.getApplicationContext();
        this.registry = context.getApplicationContext().getBean(KafkaListenerEndpointRegistry.class);
        this.groupId = groupPrefix + "." + getId();
        this.retryTopic = getTopic() + RETRY_TOPIC_POSTFIX;
        this.concurrency = Optional.ofNullable(props.getSub().get(getId())).map(OrzKafkaProps.SubConfig::getConcurrency).orElse(kafkaProperties.getListener().getConcurrency());
        this.autoStartup = Optional.ofNullable(props.getSub().get(getId())).map(OrzKafkaProps.SubConfig::isRunning).orElse(kafkaProperties.getListener().isAutoStartup());
    }
}
