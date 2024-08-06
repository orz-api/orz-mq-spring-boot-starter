package orz.springboot.mq.api.pub;

import orz.springboot.kafka.OrzKafkaProtobufPub;
import orz.springboot.mq.annotation.OrzPubApi;
import orz.springboot.mq.api.model.TestProtobufSchemaEventBo;
import orz.springboot.mq.api.model.TestProtobufV1To;

import java.util.concurrent.CompletableFuture;

@OrzPubApi(topic = "test-protobuf-schema")
public class TestProtobufSchemaPublishV1Api extends OrzKafkaProtobufPub<TestProtobufSchemaEventBo, TestProtobufV1To> {
    @Override
    protected CompletableFuture<Void> publish(TestProtobufSchemaEventBo event) {
        return publishMessage(event.toTestProtobufV1To(), extraKey(event.getLongField()));
    }
}
