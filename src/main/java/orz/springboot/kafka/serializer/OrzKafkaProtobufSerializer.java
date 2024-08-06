package orz.springboot.kafka.serializer;

import com.google.protobuf.Message;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class OrzKafkaProtobufSerializer<T extends Message> implements Serializer<T> {
    @Override
    public byte[] serialize(String topic, T data) {
        return serialize(topic, null, data);
    }

    @Override
    public byte[] serialize(String topic, Headers headers, T data) {
        if (data == null) {
            return null;
        }
        try {
            return data.toByteArray();
        } catch (Exception e) {
            throw new SerializationException("Error serializing Protobuf message", e);
        }
    }

    public static void configure(Map<String, Object> configs) {
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, OrzKafkaProtobufSerializer.class);
    }
}
