package orz.springboot.mq.kafka;

import lombok.Data;
import org.apache.kafka.common.header.Headers;

@Data
public class OrzPubKafkaExtra {
    static final OrzPubKafkaExtra EMPTY = new OrzPubKafkaExtra(null, null, null, null);

    private final String key;
    private final Integer partition;
    private final Long timestamp;
    private final Headers headers;

    public OrzPubKafkaExtra setKey(String key) {
        return new OrzPubKafkaExtra(key, partition, timestamp, headers);
    }

    public OrzPubKafkaExtra setPartition(Integer partition) {
        return new OrzPubKafkaExtra(key, partition, timestamp, headers);
    }

    public OrzPubKafkaExtra setTimestamp(Long timestamp) {
        return new OrzPubKafkaExtra(key, partition, timestamp, headers);
    }

    public OrzPubKafkaExtra setHeaders(Headers headers) {
        return new OrzPubKafkaExtra(key, partition, timestamp, headers);
    }

    public static OrzPubKafkaExtra key(String key) {
        return EMPTY.setKey(key);
    }

    public static OrzPubKafkaExtra partition(Integer partition) {
        return EMPTY.setPartition(partition);
    }

    public static OrzPubKafkaExtra timestamp(Long timestamp) {
        return EMPTY.setTimestamp(timestamp);
    }

    public static OrzPubKafkaExtra headers(Headers headers) {
        return EMPTY.setHeaders(headers);
    }
}
