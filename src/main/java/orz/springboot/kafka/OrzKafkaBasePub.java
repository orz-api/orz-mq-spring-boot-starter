package orz.springboot.kafka;

import jakarta.annotation.Nullable;
import lombok.AccessLevel;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import orz.springboot.mq.OrzMqBeanInitContext;
import orz.springboot.mq.OrzMqPub;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

@Getter(AccessLevel.PROTECTED)
public abstract class OrzKafkaBasePub<E, M> extends OrzMqPub<E, M> {
    private OrzKafkaProps props;
    private KafkaTemplate<String, M> kafkaTemplate;

    public OrzKafkaBasePub() {
    }

    public OrzKafkaBasePub(KafkaTemplate<String, M> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    protected void init(OrzMqBeanInitContext context) {
        super.init(context);

        this.props = context.getApplicationContext().getBean(OrzKafkaProps.class);

        if (this.kafkaTemplate == null) {
            var defaultProducerFactory = (DefaultKafkaProducerFactory<?, ?>) context.getApplicationContext().getBean(DefaultKafkaProducerFactory.class);
            var producerConfigs = new HashMap<>(defaultProducerFactory.getConfigurationProperties());

            var bootstrapServers = Optional.ofNullable(this.props.getPub(getId())).map(OrzKafkaProps.PubConfig::getBootstrapServers).orElse(null);
            if (StringUtils.isNotBlank(bootstrapServers)) {
                producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            }
            producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            setProducerConfigs(producerConfigs);

            this.kafkaTemplate = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(producerConfigs));
        }
    }

    protected abstract void setProducerConfigs(Map<String, Object> configs);

    protected CompletableFuture<Void> publishMessage(M message) {
        return publishMessage(message, null);
    }

    protected CompletableFuture<Void> publishMessage(M message, @Nullable OrzKafkaPubExtra extra) {
        return publishMessageWithResult(message, extra).thenRun(VOID);
    }

    protected CompletableFuture<SendResult<String, M>> publishMessageWithResult(M message, @Nullable OrzKafkaPubExtra extra) {
        if (extra == null) {
            extra = OrzKafkaPubExtra.EMPTY;
        }
        return kafkaTemplate.send(new ProducerRecord<>(
                getTopic(), extra.getPartition(), extra.getTimestamp(), extra.getKey(), message, extra.getHeaders()
        ));
    }

    protected OrzKafkaPubExtra extraKey(String key) {
        return OrzKafkaPubExtra.key(key);
    }

    protected OrzKafkaPubExtra extraKey(Integer key) {
        return extraKey(Optional.ofNullable(key).map(Object::toString).orElse(null));
    }

    protected OrzKafkaPubExtra extraKey(Long key) {
        return extraKey(Optional.ofNullable(key).map(Object::toString).orElse(null));
    }

    protected OrzKafkaPubExtra extraPartition(int partition) {
        return OrzKafkaPubExtra.partition(partition);
    }

    protected OrzKafkaPubExtra extraTimestamp(long timestamp) {
        return OrzKafkaPubExtra.timestamp(timestamp);
    }

    protected OrzKafkaPubExtra extraHeaders(Headers headers) {
        return OrzKafkaPubExtra.headers(headers);
    }
}
