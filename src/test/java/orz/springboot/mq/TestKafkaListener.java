package orz.springboot.mq;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Hex;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class TestKafkaListener {
    @KafkaListener(topics = {"test-json", "test-schema-json"}, groupId = "TestKafkaListener1")
    public void subscribeString(ConsumerRecord<String, String> record) {
        log.info("""
                        subscribe string record
                        ====================================================================================================
                        topic     : {}
                        partition : {}
                        createTime: {}
                        keySize   : {}
                        valueSize : {}
                        headers   : {}
                        key       : {}
                        value     : {}
                        ====================================================================================================""",
                record.topic(),
                record.partition(),
                record.timestamp(),
                record.serializedKeySize(),
                record.serializedValueSize(),
                record.headers(),
                record.key(),
                record.value()
        );
    }

    @KafkaListener(
            topics = {"test-protobuf", "test-schema-protobuf"},
            groupId = "TestKafkaListener2",
            properties = "value.deserializer=org.apache.kafka.common.serialization.ByteArrayDeserializer"
    )
    public void subscribeBytes(ConsumerRecord<String, byte[]> record) {
        log.info("""
                        subscribe bytes record
                        ====================================================================================================
                        topic     : {}
                        partition : {}
                        createTime: {}
                        keySize   : {}
                        valueSize : {}
                        headers   : {}
                        key       : {}
                        value     : {}
                        ====================================================================================================""",
                record.topic(),
                record.partition(),
                record.timestamp(),
                record.serializedKeySize(),
                record.serializedValueSize(),
                record.headers(),
                record.key(),
                Hex.encodeHexString(record.value())
        );
    }
}
