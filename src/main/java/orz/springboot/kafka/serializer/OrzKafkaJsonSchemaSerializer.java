package orz.springboot.kafka.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import orz.springboot.base.OrzBaseUtils;

import java.util.Map;

public class OrzKafkaJsonSchemaSerializer<T> extends KafkaJsonSchemaSerializer<T> {
    public OrzKafkaJsonSchemaSerializer() {
        super();
        this.objectMapper = createObjectMapper();
    }

    private static ObjectMapper createObjectMapper() {
        return OrzBaseUtils.getAppContext().getBean(ObjectMapper.class).copy();
    }

    public static void configure(Map<String, Object> configs, String registryUrl) {
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, OrzKafkaJsonSchemaSerializer.class);
        configs.put(KafkaJsonSchemaSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl);
        configs.put(KafkaJsonSchemaSerializerConfig.WRITE_DATES_AS_ISO8601, true);
    }
}
