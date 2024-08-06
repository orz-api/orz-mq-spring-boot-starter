package orz.springboot.kafka.serializer;

import com.google.protobuf.Message;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Map;

@SuppressWarnings("unchecked")
public class OrzKafkaProtobufDeserializer<T extends Message> implements Deserializer<T> {
    public static final String SPECIFIC_PROTOBUF_VALUE_TYPE = "specific.protobuf.value.type";

    private Method parseMethod;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        var valueType = (Class<T>) configs.get(SPECIFIC_PROTOBUF_VALUE_TYPE);
        if (valueType == null) {
            throw new InvalidConfigurationException("value deserializer must be configured with specific.protobuf.value.type");
        }
        try {
            this.parseMethod = valueType.getDeclaredMethod("parseFrom", ByteBuffer.class);
        } catch (Exception e) {
            throw new ConfigException("Class " + valueType.getCanonicalName() + " is not a valid protobuf message class", e);
        }
    }

    @Override
    public T deserialize(String s, byte[] bytes) {
        return deserialize(s, null, bytes);
    }

    @Override
    public T deserialize(String topic, Headers headers, byte[] data) {
        try {
            return (T) parseMethod.invoke(null, ByteBuffer.wrap(data));
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new SerializationException("Not a valid protobuf builder");
        }
    }

    public static void configure(Map<String, Object> configs, Class<?> messageType) {
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, OrzKafkaProtobufDeserializer.class);
        configs.put(OrzKafkaProtobufDeserializer.SPECIFIC_PROTOBUF_VALUE_TYPE, messageType);
    }
}
