package orz.springboot.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Nullable;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import orz.springboot.base.OrzBaseUtils;
import orz.springboot.mq.OrzMqBeanInitContext;
import orz.springboot.mq.OrzMqPub;

import java.util.concurrent.CompletableFuture;

@Getter(AccessLevel.PROTECTED)
public abstract class OrzKafkaPub<E> extends OrzMqPub<E> {
    private KafkaTemplate<String, String> kafkaTemplate;

    public OrzKafkaPub() {
        super();
    }

    public OrzKafkaPub(ObjectMapper objectMapper, KafkaTemplate<String, String> kafkaTemplate) {
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

    protected CompletableFuture<Void> publishObject(Object data) {
        return publishObject(data, null);
    }

    protected CompletableFuture<Void> publishObject(Object data, @Nullable OrzKafkaPubExtra extra) {
        return publishObjectWithResult(data, extra).thenRun(VOID);
    }

    protected CompletableFuture<Void> publishString(String data) {
        return publishStringWithResult(data, null).thenRun(VOID);
    }

    @SneakyThrows
    protected CompletableFuture<SendResult<String, String>> publishObjectWithResult(Object data, @Nullable OrzKafkaPubExtra extra) {
        return publishStringWithResult(getObjectMapper().writeValueAsString(data), extra);
    }

    protected CompletableFuture<SendResult<String, String>> publishStringWithResult(String data, @Nullable OrzKafkaPubExtra extra) {
        if (extra == null) {
            extra = OrzKafkaPubExtra.EMPTY;
        }
        return kafkaTemplate.send(new ProducerRecord<>(
                getTopic(), extra.getPartition(), extra.getTimestamp(), extra.getKey(), data, extra.getHeaders()
        ));
    }

    protected OrzKafkaPubExtra extraKey(String key) {
        return OrzKafkaPubExtra.key(key);
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
