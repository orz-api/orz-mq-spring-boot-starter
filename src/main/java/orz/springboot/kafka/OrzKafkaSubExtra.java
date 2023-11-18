package orz.springboot.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class OrzKafkaSubExtra {
    private final ConsumerRecord<String, String> record;

    public OrzKafkaSubExtra(ConsumerRecord<String, String> record) {
        this.record = record;
    }

    public String getTopic() {
        return record.topic();
    }
}
