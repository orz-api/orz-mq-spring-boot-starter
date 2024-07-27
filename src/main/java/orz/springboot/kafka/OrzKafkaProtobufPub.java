package orz.springboot.kafka;

import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializerConfig;
import lombok.AccessLevel;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Map;

@Getter(AccessLevel.PROTECTED)
public abstract class OrzKafkaProtobufPub<E, M> extends OrzKafkaBasePub<E, M> {
    public OrzKafkaProtobufPub() {
    }

    public OrzKafkaProtobufPub(KafkaTemplate<String, M> kafkaTemplate) {
        super(kafkaTemplate);
    }

    @Override
    protected void setProducerConfigs(Map<String, Object> configs) {
        var pubConfig = getProps().getPub().get(getId());
        if (pubConfig != null && StringUtils.isNotBlank(pubConfig.getSchemaRegistryUrl())) {
            configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class);
            configs.put(KafkaProtobufSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, pubConfig.getSchemaRegistryUrl());
        } else {
            throw new UnsupportedOperationException("Protobuf without schema registry is not supported");
        }
    }
}
