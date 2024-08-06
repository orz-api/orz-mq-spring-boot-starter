package orz.springboot.kafka;

import lombok.AccessLevel;
import lombok.Getter;
import org.springframework.kafka.core.KafkaTemplate;
import orz.springboot.kafka.serializer.OrzKafkaJsonSchemaSerializer;
import orz.springboot.kafka.serializer.OrzKafkaJsonSerializer;

import java.util.Map;

@Getter(AccessLevel.PROTECTED)
public abstract class OrzKafkaJsonPub<E, M> extends OrzKafkaBasePub<E, M> {
    public OrzKafkaJsonPub() {
    }

    public OrzKafkaJsonPub(KafkaTemplate<String, M> kafkaTemplate) {
        super(kafkaTemplate);
    }

    @Override
    protected void configureProducer(Map<String, Object> configs) {
        var registry = getProps().getPubSchemaRegistry(getId());
        if (registry != null) {
            OrzKafkaJsonSchemaSerializer.configure(configs, registry.getUrl());
        } else {
            OrzKafkaJsonSerializer.configure(configs);
        }
    }
}
