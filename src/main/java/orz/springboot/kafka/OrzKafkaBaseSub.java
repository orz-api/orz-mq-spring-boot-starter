package orz.springboot.kafka;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.FatalBeanException;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import orz.springboot.alarm.exception.OrzAlarmException;
import orz.springboot.kafka.model.OrzKafkaSubRunningChangeEventBo;
import orz.springboot.mq.OrzMqBeanInitContext;
import orz.springboot.mq.OrzMqSub;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static orz.springboot.alarm.OrzAlarmUtils.alarm;
import static orz.springboot.base.description.OrzDescriptionUtils.desc;
import static orz.springboot.kafka.OrzKafkaConstants.RETRY_GROUP_ID_HEADER;
import static orz.springboot.kafka.OrzKafkaConstants.RETRY_TOPIC_POSTFIX;

@Slf4j
@Getter(AccessLevel.PROTECTED)
@KafkaListener(
        id = "#{__listener.id}",
        containerFactory = "#{__listener.containerFactory}",
        groupId = "#{__listener.groupId}",
        topics = {
                "#{__listener.topic}",
                "#{__listener.retryTopic}",
        },
        concurrency = "#{__listener.concurrency}",
        autoStartup = "#{__listener.autoStartup}"
)
public abstract class OrzKafkaBaseSub<M> extends OrzMqSub<M, OrzKafkaSubExtra<M>> {
    private OrzKafkaProps props;
    private ApplicationEventPublisher publisher;
    private KafkaListenerEndpointRegistry registry;
    private String groupId;
    private String retryTopic;
    private Integer concurrency;
    private Boolean autoStartup;

    public OrzKafkaBaseSub() {
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

    public String getContainerFactory() {
        return getId() + "ContainerFactory";
    }

    @KafkaHandler(isDefault = true)
    public void subscribe(ConsumerRecord<String, M> record) {
        if (record.topic().endsWith(RETRY_TOPIC_POSTFIX)) {
            var retryGroupId = Optional.ofNullable(record.headers().lastHeader(RETRY_GROUP_ID_HEADER))
                    .map(header -> new String(header.value(), StandardCharsets.UTF_8))
                    .orElse(null);
            if (!Objects.equals(retryGroupId, getGroupId())) {
                log.info(desc("discard retry record", "groupId", getGroupId(), "retryGroupId", retryGroupId));
                return;
            }
        }
        try {
            subscribe(record.value(), new OrzKafkaSubExtra<>(record));
        } catch (OrzAlarmException e) {
            alarm(e, e);
            throw e;
        }
    }

    @Override
    public void start() {
        var container = registry.getListenerContainer(getId());
        if (container == null) {
            throw new RuntimeException(desc("listener container is null", "id", getId()));
        }
        if (!container.isRunning()) {
            log.info(desc("kafka sub start", "id", getId()));
            container.start();
            publisher.publishEvent(new OrzKafkaSubRunningChangeEventBo(this, true));
        }
    }

    @Override
    public void stop() {
        var container = registry.getListenerContainer(getId());
        if (container == null) {
            throw new RuntimeException(desc("listener container is null", "id", getId()));
        }
        if (container.isRunning()) {
            log.info(desc("kafka sub stop", "id", getId()));
            container.stop();
            publisher.publishEvent(new OrzKafkaSubRunningChangeEventBo(this, false));
        }
    }

    @Override
    protected void init(OrzMqBeanInitContext context) {
        super.init(context);

        this.props = context.getApplicationContext().getBean(OrzKafkaProps.class);

        var defaultConsumerFactory = (DefaultKafkaConsumerFactory<?, ?>) context.getApplicationContext().getBean(DefaultKafkaConsumerFactory.class);
        var consumerConfigs = new HashMap<>(defaultConsumerFactory.getConfigurationProperties());

        var consumerGroup = (String) consumerConfigs.get(ConsumerConfig.GROUP_ID_CONFIG);
        if (consumerGroup == null) {
            consumerGroup = context.getApplicationContext().getEnvironment().getProperty("spring.application.name");
        }
        if (StringUtils.isBlank(consumerGroup)) {
            throw new FatalBeanException(desc("kafka consumer group is blank"));
        }

        this.publisher = context.getApplicationContext();
        this.registry = context.getApplicationContext().getBean(KafkaListenerEndpointRegistry.class);
        this.groupId = StringUtils.isBlank(getQualifier()) ? consumerGroup : consumerGroup + "." + getQualifier();
        this.retryTopic = getTopic() + RETRY_TOPIC_POSTFIX;

        var kafkaProperties = context.getApplicationContext().getBean(KafkaProperties.class);
        this.concurrency = Optional.ofNullable(this.props.getSub(getId())).map(OrzKafkaProps.SubConfig::getConcurrency).orElse(kafkaProperties.getListener().getConcurrency());
        this.autoStartup = Optional.ofNullable(this.props.getSub(getId())).map(OrzKafkaProps.SubConfig::isRunning).orElse(kafkaProperties.getListener().isAutoStartup());

        var bootstrapServers = Optional.ofNullable(this.props.getSub(getId())).map(OrzKafkaProps.SubConfig::getBootstrapServers).orElse(null);
        if (StringUtils.isNotBlank(bootstrapServers)) {
            consumerConfigs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        }
        consumerConfigs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        setConsumerConfigs(consumerConfigs);

        var containerFactory = new ConcurrentKafkaListenerContainerFactory<String, M>();
        containerFactory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(consumerConfigs));
        context.getApplicationContext().getBeanFactory().registerSingleton(getContainerFactory(), containerFactory);
    }

    protected abstract void setConsumerConfigs(Map<String, Object> configs);
}
