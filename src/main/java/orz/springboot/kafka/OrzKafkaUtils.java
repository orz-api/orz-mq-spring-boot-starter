package orz.springboot.kafka;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import orz.springboot.mq.OrzMqBeanInitContext;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

public class OrzKafkaUtils {
    public static <T> KafkaTemplate<String, T> createKafkaTemplate(OrzMqBeanInitContext context, Consumer<Map<String, Object>> configsEditor) {
        var defaultProducerFactory = (DefaultKafkaProducerFactory<?, ?>) context.getApplicationContext().getBean(DefaultKafkaProducerFactory.class);
        var producerConfigs = new HashMap<>(defaultProducerFactory.getConfigurationProperties());

        producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configsEditor.accept(producerConfigs);

        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(producerConfigs));
    }
}
