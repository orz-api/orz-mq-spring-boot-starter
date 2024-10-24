package orz.springboot.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Map;

public abstract class OrzKafkaStringSub extends OrzKafkaBaseSub<String> {
    public OrzKafkaStringSub() {
    }

    @Override
    protected Class<String> obtainMessageType() {
        return String.class;
    }

    @Override
    protected void configureConsumer(Map<String, Object> configs) {
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    }

    @Override
    protected void configureDltProducer(Map<String, Object> configs) {
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    }
}
