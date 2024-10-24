package orz.springboot.mq.api.pub;

import orz.springboot.kafka.OrzKafkaStringPub;
import orz.springboot.mq.annotation.OrzPubApi;
import orz.springboot.mq.api.model.TestStringEventBo;

import java.util.concurrent.CompletableFuture;

@OrzPubApi(topic = "test-string")
public class TestStringPublishV1Api extends OrzKafkaStringPub<TestStringEventBo> {
    @Override
    protected CompletableFuture<Void> publish(TestStringEventBo event) {
        return publishMessage(event.getMessage(), extraKey(event.getKey()));
    }
}
