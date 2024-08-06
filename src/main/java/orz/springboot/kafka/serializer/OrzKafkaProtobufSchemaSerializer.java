package orz.springboot.kafka.serializer;

import com.google.protobuf.Message;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Map;

public class OrzKafkaProtobufSchemaSerializer<T extends Message> extends KafkaProtobufSerializer<T> {
    public OrzKafkaProtobufSchemaSerializer() {
        super();
    }

    public static void configure(Map<String, Object> configs, String registryUrl) {
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, OrzKafkaProtobufSchemaSerializer.class);
        configs.put(KafkaProtobufSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl);
    }
}
