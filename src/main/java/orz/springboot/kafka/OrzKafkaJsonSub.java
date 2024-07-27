package orz.springboot.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import orz.springboot.base.OrzBaseUtils;

import java.util.Map;

public abstract class OrzKafkaJsonSub<M> extends OrzKafkaBaseSub<M> {
    public OrzKafkaJsonSub() {
    }

    @Override
    protected void setConsumerConfigs(Map<String, Object> configs) {
        var registry = getProps().getSubSchemaRegistry(getId());
        if (registry != null) {
            configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, OrzKafkaJsonSchemaDeserializer.class);
            configs.put(KafkaJsonSchemaDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, registry.getUrl());
            configs.put(KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE, getMessageType());
        } else {
            configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, OrzKafkaJsonDeserializer.class);
            configs.put(OrzKafkaJsonDeserializer.USE_TYPE_INFO_HEADERS, false);
            configs.put(OrzKafkaJsonDeserializer.VALUE_DEFAULT_TYPE, getMessageType());
        }
    }

    public static class OrzKafkaJsonDeserializer<T> extends JsonDeserializer<T> {
        public OrzKafkaJsonDeserializer() {
            super(createObjectMapper());
        }

        private static ObjectMapper createObjectMapper() {
            return OrzBaseUtils.getAppContext().getBean(ObjectMapper.class).copy();
        }
    }

    public static class OrzKafkaJsonSchemaDeserializer<T> extends KafkaJsonSchemaDeserializer<T> {
        public OrzKafkaJsonSchemaDeserializer() {
            super();
            this.objectMapper = createObjectMapper();
        }

        private static ObjectMapper createObjectMapper() {
            return OrzBaseUtils.getAppContext().getBean(ObjectMapper.class).copy();
        }
    }
}
