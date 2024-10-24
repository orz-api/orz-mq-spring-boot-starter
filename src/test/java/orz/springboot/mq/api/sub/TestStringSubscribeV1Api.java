package orz.springboot.mq.api.sub;

import orz.springboot.kafka.OrzKafkaStringSub;
import orz.springboot.kafka.OrzKafkaSubExtra;
import orz.springboot.mq.annotation.OrzSubApi;

import java.util.Optional;

@SuppressWarnings("OptionalAssignedToNull")
@OrzSubApi(topic = "test-string")
public class TestStringSubscribeV1Api extends OrzKafkaStringSub {
    private Optional<String> lastMessage;
    private Optional<String> lastKey;

    @Override
    protected void subscribe(String message, OrzKafkaSubExtra<String> extra) {
        synchronized (this) {
            lastMessage = Optional.ofNullable(message);
            lastKey = Optional.ofNullable(extra.getKey());
            this.notifyAll();
            if ("TestDlt".equals(message)) {
                throw new RuntimeException("TestDlt");
            }
        }
    }

    public synchronized String takeLastMessage() {
        if (lastMessage == null) {
            try {
                this.wait(5000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        var result = lastMessage;
        lastMessage = null;
        if (result != null && result.isPresent()) {
            return result.get();
        }
        return null;
    }

    public synchronized String takeLastKey() {
        if (lastKey == null) {
            try {
                this.wait(5000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        var result = lastKey;
        lastKey = null;
        if (result != null && result.isPresent()) {
            return result.get();
        }
        return null;
    }
}
