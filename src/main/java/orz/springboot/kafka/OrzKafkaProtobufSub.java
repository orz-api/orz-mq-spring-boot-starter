package orz.springboot.kafka;

import com.google.protobuf.Message;
import orz.springboot.kafka.serializer.OrzKafkaProtobufDeserializer;
import orz.springboot.kafka.serializer.OrzKafkaProtobufSchemaDeserializer;
import orz.springboot.kafka.serializer.OrzKafkaProtobufSchemaSerializer;
import orz.springboot.kafka.serializer.OrzKafkaProtobufSerializer;

import java.util.Map;

public abstract class OrzKafkaProtobufSub<M extends Message> extends OrzKafkaBaseSub<M> {
    public OrzKafkaProtobufSub() {
    }

    @Override
    protected void configureConsumer(Map<String, Object> configs) {
        var registry = getProps().getSubSchemaRegistry(getId());
        if (registry != null) {
            OrzKafkaProtobufSchemaDeserializer.configure(configs, getMessageType(), registry.getUrl());
        } else {
            OrzKafkaProtobufDeserializer.configure(configs, getMessageType());
        }
    }

    @Override
    protected void configureDltProducer(Map<String, Object> configs) {
        var registry = getProps().getSubDltPubSchemaRegistry(getId());
        if (registry != null) {
            OrzKafkaProtobufSchemaSerializer.configure(configs, registry.getUrl());
        } else {
            OrzKafkaProtobufSerializer.configure(configs);
        }
    }
}
