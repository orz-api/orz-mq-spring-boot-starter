package orz.springboot.mq.api.sub;

import orz.springboot.kafka.OrzKafkaProtobufSub;
import orz.springboot.kafka.OrzKafkaSubExtra;
import orz.springboot.mq.annotation.OrzSubApi;
import orz.springboot.mq.api.model.TestProtobufV1To;

import java.util.Optional;

@SuppressWarnings("OptionalAssignedToNull")
@OrzSubApi(topic = "test-protobuf-schema")
public class TestProtobufSchemaSubscribeV1Api extends OrzKafkaProtobufSub<TestProtobufV1To> {
    private Optional<TestProtobufV1To> lastMessage;
    private Optional<String> lastKey;

    @Override
    protected void subscribe(TestProtobufV1To message, OrzKafkaSubExtra<TestProtobufV1To> extra) {
        synchronized (this) {
            lastMessage = Optional.ofNullable(message);
            lastKey = Optional.ofNullable(extra.getKey());
            this.notifyAll();
            if (message != null && "TestDlt".equals(message.getStrField())) {
                throw new RuntimeException("TestDlt");
            }
        }
    }

    public synchronized TestProtobufV1To takeLastMessage() {
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
