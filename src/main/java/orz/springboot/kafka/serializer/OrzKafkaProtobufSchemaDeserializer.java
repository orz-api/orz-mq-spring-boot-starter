package orz.springboot.kafka.serializer;

import com.google.protobuf.Message;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Map;

public class OrzKafkaProtobufSchemaDeserializer<T extends Message> extends KafkaProtobufDeserializer<T> {
    public OrzKafkaProtobufSchemaDeserializer() {
        super();
    }

    public static void configure(Map<String, Object> configs, Class<?> messageType, String registryUrl) {
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, OrzKafkaProtobufSchemaDeserializer.class);
        configs.put(KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE, messageType);
        configs.put(KafkaProtobufDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl);
    }
}
