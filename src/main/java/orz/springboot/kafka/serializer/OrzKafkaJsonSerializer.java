package orz.springboot.kafka.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.kafka.support.serializer.JsonSerializer;
import orz.springboot.base.OrzBaseUtils;

import java.util.Map;

public class OrzKafkaJsonSerializer<T> extends JsonSerializer<T> {
    public OrzKafkaJsonSerializer() {
        super(createObjectMapper());
    }

    private static ObjectMapper createObjectMapper() {
        return OrzBaseUtils.getAppContext().getBean(ObjectMapper.class).copy();
    }

    public static void configure(Map<String, Object> configs) {
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, OrzKafkaJsonSerializer.class);
        configs.put(OrzKafkaJsonSerializer.ADD_TYPE_INFO_HEADERS, false);
    }
}
