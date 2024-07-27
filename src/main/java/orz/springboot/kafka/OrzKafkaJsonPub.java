package orz.springboot.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializerConfig;
import lombok.AccessLevel;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerializer;
import orz.springboot.base.OrzBaseUtils;

import java.util.Map;

@Getter(AccessLevel.PROTECTED)
public abstract class OrzKafkaJsonPub<E, M> extends OrzKafkaBasePub<E, M> {
    public OrzKafkaJsonPub() {
    }

    public OrzKafkaJsonPub(KafkaTemplate<String, M> kafkaTemplate) {
        super(kafkaTemplate);
    }

    @Override
    protected void setProducerConfigs(Map<String, Object> configs) {
        var pubConfig = getProps().getPub().get(getId());
        if (pubConfig != null && StringUtils.isNotBlank(pubConfig.getSchemaRegistryUrl())) {
            configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, OrzKafkaJsonSchemaSerializer.class);
            configs.put(KafkaJsonSchemaSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, pubConfig.getSchemaRegistryUrl());
            configs.put(KafkaJsonSchemaSerializerConfig.WRITE_DATES_AS_ISO8601, true);
        } else {
            configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, OrzKafkaJsonSerializer.class);
            configs.put(OrzKafkaJsonSerializer.ADD_TYPE_INFO_HEADERS, false);
        }
    }

    public static class OrzKafkaJsonSerializer<T> extends JsonSerializer<T> {
        public OrzKafkaJsonSerializer() {
            super(createObjectMapper());
        }

        private static ObjectMapper createObjectMapper() {
            return OrzBaseUtils.getAppContext().getBean(ObjectMapper.class).copy();
        }
    }

    public static class OrzKafkaJsonSchemaSerializer<T> extends KafkaJsonSchemaSerializer<T> {
        public OrzKafkaJsonSchemaSerializer() {
            super();
            this.objectMapper = createObjectMapper();
        }

        private static ObjectMapper createObjectMapper() {
            return OrzBaseUtils.getAppContext().getBean(ObjectMapper.class).copy();
        }
    }
}
