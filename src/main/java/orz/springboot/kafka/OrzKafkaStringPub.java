package orz.springboot.kafka;

import lombok.AccessLevel;
import lombok.Getter;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Map;

@Getter(AccessLevel.PROTECTED)
public abstract class OrzKafkaStringPub<E> extends OrzKafkaBasePub<E, String> {
    public OrzKafkaStringPub() {
    }

    public OrzKafkaStringPub(KafkaTemplate<String, String> kafkaTemplate) {
        super(kafkaTemplate);
    }

    @Override
    protected Class<String> obtainMessageType() {
        return String.class;
    }

    @Override
    protected void configureProducer(Map<String, Object> configs) {
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    }
}
