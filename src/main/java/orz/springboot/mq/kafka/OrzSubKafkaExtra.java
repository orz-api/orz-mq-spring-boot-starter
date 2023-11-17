package orz.springboot.mq.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class OrzSubKafkaExtra {
    private final ConsumerRecord<String, String> record;

    public OrzSubKafkaExtra(ConsumerRecord<String, String> record) {
        this.record = record;
    }

    public String getTopic() {
        return record.topic();
    }
}
