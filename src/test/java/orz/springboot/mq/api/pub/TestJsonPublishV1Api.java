package orz.springboot.mq.api.pub;

import orz.springboot.kafka.OrzKafkaJsonPub;
import orz.springboot.mq.annotation.OrzPubApi;
import orz.springboot.mq.api.model.TestJsonEventBo;
import orz.springboot.mq.api.model.TestJsonV1To;

import java.util.concurrent.CompletableFuture;

@OrzPubApi(topic = "test-json")
public class TestJsonPublishV1Api extends OrzKafkaJsonPub<TestJsonEventBo, TestJsonV1To> {
    @Override
    protected CompletableFuture<Void> publish(TestJsonEventBo event) {
        return publishMessage(event.toTestJsonV1To(), extraKey(event.getLongObjField()));
    }
}
