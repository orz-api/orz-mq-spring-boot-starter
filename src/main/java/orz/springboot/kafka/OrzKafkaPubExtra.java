package orz.springboot.kafka;

import lombok.Data;
import org.apache.kafka.common.header.Headers;

@Data
public class OrzKafkaPubExtra {
    static final OrzKafkaPubExtra EMPTY = new OrzKafkaPubExtra(null, null, null, null);

    private final String key;
    private final Integer partition;
    private final Long timestamp;
    private final Headers headers;

    public OrzKafkaPubExtra setKey(String key) {
        return new OrzKafkaPubExtra(key, partition, timestamp, headers);
    }

    public OrzKafkaPubExtra setPartition(Integer partition) {
        return new OrzKafkaPubExtra(key, partition, timestamp, headers);
    }

    public OrzKafkaPubExtra setTimestamp(Long timestamp) {
        return new OrzKafkaPubExtra(key, partition, timestamp, headers);
    }

    public OrzKafkaPubExtra setHeaders(Headers headers) {
        return new OrzKafkaPubExtra(key, partition, timestamp, headers);
    }

    public static OrzKafkaPubExtra key(String key) {
        return EMPTY.setKey(key);
    }

    public static OrzKafkaPubExtra partition(Integer partition) {
        return EMPTY.setPartition(partition);
    }

    public static OrzKafkaPubExtra timestamp(Long timestamp) {
        return EMPTY.setTimestamp(timestamp);
    }

    public static OrzKafkaPubExtra headers(Headers headers) {
        return EMPTY.setHeaders(headers);
    }
}
