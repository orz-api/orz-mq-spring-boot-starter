package orz.springboot.kafka.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import orz.springboot.base.OrzBaseUtils;

import java.util.Map;

public class OrzKafkaJsonSchemaDeserializer<T> extends KafkaJsonSchemaDeserializer<T> {
    public OrzKafkaJsonSchemaDeserializer() {
        super();
        this.objectMapper = createObjectMapper();
    }

    private static ObjectMapper createObjectMapper() {
        return OrzBaseUtils.getAppContext().getBean(ObjectMapper.class).copy();
    }

    public static void configure(Map<String, Object> configs, Class<?> messageType, String registryUrl) {
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, OrzKafkaJsonSchemaDeserializer.class);
        configs.put(KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE, messageType);
        configs.put(KafkaJsonSchemaDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl);
    }
}
