package orz.springboot.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class OrzKafkaSubExtra<R> {
    private final ConsumerRecord<String, R> record;

    public OrzKafkaSubExtra(ConsumerRecord<String, R> record) {
        this.record = record;
    }

    public String getTopic() {
        return record.topic();
    }

    public String getKey() {
        return record.key();
    }
}
