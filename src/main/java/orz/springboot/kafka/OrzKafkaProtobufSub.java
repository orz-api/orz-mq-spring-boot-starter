package orz.springboot.kafka;

import com.google.protobuf.Message;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Map;

public abstract class OrzKafkaProtobufSub<M extends Message> extends OrzKafkaBaseSub<M> {
    public OrzKafkaProtobufSub() {
    }

    @Override
    protected void setConsumerConfigs(Map<String, Object> configs) {
        var subConfig = getProps().getSub().get(getId());
        if (subConfig != null && StringUtils.isNotBlank(subConfig.getSchemaRegistryUrl())) {
            configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaProtobufDeserializer.class);
            configs.put(KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE, getMessageType());
            configs.put(KafkaProtobufDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, subConfig.getSchemaRegistryUrl());
        } else {
            throw new UnsupportedOperationException("Protobuf without schema registry is not supported");
        }
    }
}
