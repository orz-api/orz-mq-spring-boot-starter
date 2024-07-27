package orz.springboot.mq.api.pub;

import orz.springboot.kafka.OrzKafkaJsonPub;
import orz.springboot.mq.annotation.OrzPubApi;
import orz.springboot.mq.api.model.TestJsonV1To;
import orz.springboot.mq.api.model.TestSchemaJsonEventBo;

import java.util.concurrent.CompletableFuture;

@OrzPubApi(topic = "test-schema-json")
public class TestSchemaJsonPublishV1Api extends OrzKafkaJsonPub<TestSchemaJsonEventBo, TestJsonV1To> {
    @Override
    protected CompletableFuture<Void> publish(TestSchemaJsonEventBo event) {
        return publishMessage(event.toTestJsonV1To(), extraKey(event.getLongObjField()));
    }
}
