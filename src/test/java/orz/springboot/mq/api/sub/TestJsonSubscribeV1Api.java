package orz.springboot.mq.api.sub;

import orz.springboot.kafka.OrzKafkaJsonSub;
import orz.springboot.kafka.OrzKafkaSubExtra;
import orz.springboot.mq.annotation.OrzSubApi;
import orz.springboot.mq.api.model.TestJsonV1To;

import java.util.Optional;

@SuppressWarnings("OptionalAssignedToNull")
@OrzSubApi(topic = "test-json")
public class TestJsonSubscribeV1Api extends OrzKafkaJsonSub<TestJsonV1To> {
    private Optional<TestJsonV1To> lastMessage;
    private Optional<String> lastKey;

    @Override
    protected void subscribe(TestJsonV1To message, OrzKafkaSubExtra<TestJsonV1To> extra) {
        synchronized (this) {
            lastMessage = Optional.ofNullable(message);
            lastKey = Optional.ofNullable(extra.getKey());
            this.notifyAll();
        }
    }

    public synchronized TestJsonV1To takeLastMessage() {
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
