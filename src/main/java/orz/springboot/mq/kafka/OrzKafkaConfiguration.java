package orz.springboot.mq.kafka;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;

@Configuration
@ConditionalOnClass(KafkaTemplate.class)
public class OrzKafkaConfiguration {
    @Bean
    public CommonErrorHandler kafkaCommonErrorHandler(
            OrzKafkaProps props,
            KafkaProperties kafkaProperties,
            KafkaOperations<?, ?> kafkaOperations,
            @Value("${spring.application.name:unnamed}") String appName
    ) {
        var recoverer = new DeadLetterPublishingRecoverer(kafkaOperations, (r, e) -> {
            var consumerGroupId = (String) null;
            if (e instanceof ListenerExecutionFailedException ex) {
                consumerGroupId = ex.getGroupId();
            }
            if (StringUtils.isBlank(consumerGroupId)) {
                consumerGroupId = StringUtils.defaultIfBlank(kafkaProperties.getConsumer().getGroupId(), appName) + ".UNKNOWN";
            }
            // 分区设为 -1，让 kafka 根据 key 自动分配分区
            return new TopicPartition(r.topic() + ".DLT." + consumerGroupId, -1);
        });
        recoverer.setFailIfSendResultIsError(true);
        return new DefaultErrorHandler(recoverer, props.getBackOff());
    }
}
