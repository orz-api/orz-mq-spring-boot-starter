package orz.springboot.mq.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import orz.springboot.base.OrzBaseUtils;
import orz.springboot.mq.OrzMqBeanInitContext;
import orz.springboot.mq.OrzPubBase;

import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;

@Getter(AccessLevel.PROTECTED)
public abstract class OrzPubKafka<D> extends OrzPubBase<D> {
    private KafkaTemplate<String, String> kafkaTemplate;

    public OrzPubKafka() {
        super();
    }

    public OrzPubKafka(ObjectMapper objectMapper, KafkaTemplate<String, String> kafkaTemplate) {
        super(objectMapper);
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    protected void init(OrzMqBeanInitContext context) {
        super.init(context);

        if (this.kafkaTemplate == null) {
            // noinspection unchecked
            this.kafkaTemplate = (KafkaTemplate<String, String>) OrzBaseUtils.getAppContext().getBean(KafkaTemplate.class);
        }
    }

    protected CompletableFuture<Void> publishObject(String topic, Object message) {
        return publishObject(topic, message, null);
    }

    protected CompletableFuture<Void> publishObject(String topic, Object message, @Nullable OrzPubKafkaExtra extra) {
        return publishObjectWithResult(topic, message, extra).thenRun(VOID);
    }

    protected CompletableFuture<Void> publishString(String topic, String message) {
        return publishStringWithResult(topic, message, null).thenRun(VOID);
    }

    @SneakyThrows
    protected CompletableFuture<SendResult<String, String>> publishObjectWithResult(String topic, Object message, @Nullable OrzPubKafkaExtra extra) {
        return publishStringWithResult(topic, getObjectMapper().writeValueAsString(message), extra);
    }

    protected CompletableFuture<SendResult<String, String>> publishStringWithResult(String topic, String message, @Nullable OrzPubKafkaExtra extra) {
        if (extra == null) {
            extra = OrzPubKafkaExtra.EMPTY;
        }
        return this.kafkaTemplate.send(new ProducerRecord<>(
                topic, extra.getPartition(), extra.getTimestamp(), extra.getKey(), message, extra.getHeaders()
        ));
    }

    protected OrzPubKafkaExtra extraKey(String key) {
        return OrzPubKafkaExtra.key(key);
    }

    protected OrzPubKafkaExtra extraPartition(int partition) {
        return OrzPubKafkaExtra.partition(partition);
    }

    protected OrzPubKafkaExtra extraTimestamp(long timestamp) {
        return OrzPubKafkaExtra.timestamp(timestamp);
    }

    protected OrzPubKafkaExtra extraHeaders(Headers headers) {
        return OrzPubKafkaExtra.headers(headers);
    }
}
