package orz.springboot.kafka;

import orz.springboot.kafka.serializer.OrzKafkaJsonDeserializer;
import orz.springboot.kafka.serializer.OrzKafkaJsonSchemaDeserializer;
import orz.springboot.kafka.serializer.OrzKafkaJsonSchemaSerializer;
import orz.springboot.kafka.serializer.OrzKafkaJsonSerializer;

import java.util.Map;

public abstract class OrzKafkaJsonSub<M> extends OrzKafkaBaseSub<M> {
    public OrzKafkaJsonSub() {
    }

    @Override
    protected void configureConsumer(Map<String, Object> configs) {
        var registry = getProps().getSubSchemaRegistry(getId());
        if (registry != null) {
            OrzKafkaJsonSchemaDeserializer.configure(configs, getMessageType(), registry.getUrl());
        } else {
            OrzKafkaJsonDeserializer.configure(configs, getMessageType());
        }
    }

    @Override
    protected void configureDltProducer(Map<String, Object> configs) {
        var registry = getProps().getSubDltPubSchemaRegistry(getId());
        if (registry != null) {
            OrzKafkaJsonSchemaSerializer.configure(configs, registry.getUrl());
        } else {
            OrzKafkaJsonSerializer.configure(configs);
        }
    }
}
