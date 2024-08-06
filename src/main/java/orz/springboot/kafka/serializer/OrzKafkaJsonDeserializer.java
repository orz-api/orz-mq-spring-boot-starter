package orz.springboot.kafka.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import orz.springboot.base.OrzBaseUtils;

import java.util.Map;

public class OrzKafkaJsonDeserializer<T> extends JsonDeserializer<T> {
    public OrzKafkaJsonDeserializer() {
        super(createObjectMapper());
    }

    private static ObjectMapper createObjectMapper() {
        return OrzBaseUtils.getAppContext().getBean(ObjectMapper.class).copy();
    }

    public static void configure(Map<String, Object> configs, Class<?> messageType) {
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, OrzKafkaJsonDeserializer.class);
        configs.put(OrzKafkaJsonDeserializer.VALUE_DEFAULT_TYPE, messageType);
        configs.put(OrzKafkaJsonDeserializer.USE_TYPE_INFO_HEADERS, false);
    }
}
