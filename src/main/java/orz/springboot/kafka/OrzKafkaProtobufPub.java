package orz.springboot.kafka;

import com.google.protobuf.Message;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializerConfig;
import lombok.AccessLevel;
import lombok.Getter;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;
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
        var registry = getProps().getPubSchemaRegistry(getId());
        if (registry != null) {
            configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class);
            configs.put(KafkaProtobufSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, registry.getUrl());
        } else {
            configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, OrzKafkaProtobufSerializer.class);
        }
    }

    public static class OrzKafkaProtobufSerializer<T extends Message> implements Serializer<T> {
        @Override
        public byte[] serialize(String topic, T data) {
            return serialize(topic, null, data);
        }

        @Override
        public byte[] serialize(String topic, Headers headers, T data) {
            if (data == null) {
                return null;
            }
            try {
                return data.toByteArray();
            } catch (Exception e) {
                throw new SerializationException("Error serializing Protobuf message", e);
            }
        }
    }
}
