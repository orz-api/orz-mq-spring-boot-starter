package orz.springboot.mq.api.pub;

import orz.springboot.kafka.OrzKafkaJsonPub;
import orz.springboot.mq.annotation.OrzPubApi;
import orz.springboot.mq.api.model.TestJsonSchemaEventBo;
import orz.springboot.mq.api.model.TestJsonV1To;

import java.util.concurrent.CompletableFuture;

@OrzPubApi(topic = "test-json-schema")
public class TestJsonSchemaPublishV1Api extends OrzKafkaJsonPub<TestJsonSchemaEventBo, TestJsonV1To> {
    @Override
    protected CompletableFuture<Void> publish(TestJsonSchemaEventBo event) {
        return publishMessage(event.toTestJsonV1To(), extraKey(event.getLongObjField()));
    }
}
