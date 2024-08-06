package orz.springboot.kafka;

import lombok.AccessLevel;
import lombok.Getter;
import org.springframework.kafka.core.KafkaTemplate;
import orz.springboot.kafka.serializer.OrzKafkaProtobufSchemaSerializer;
import orz.springboot.kafka.serializer.OrzKafkaProtobufSerializer;

import java.util.Map;

@Getter(AccessLevel.PROTECTED)
public abstract class OrzKafkaProtobufPub<E, M> extends OrzKafkaBasePub<E, M> {
    public OrzKafkaProtobufPub() {
    }

    public OrzKafkaProtobufPub(KafkaTemplate<String, M> kafkaTemplate) {
        super(kafkaTemplate);
    }

    @Override
    protected void configureProducer(Map<String, Object> configs) {
        var registry = getProps().getPubSchemaRegistry(getId());
        if (registry != null) {
            OrzKafkaProtobufSchemaSerializer.configure(configs, registry.getUrl());
        } else {
            OrzKafkaProtobufSerializer.configure(configs);
        }
    }
}
