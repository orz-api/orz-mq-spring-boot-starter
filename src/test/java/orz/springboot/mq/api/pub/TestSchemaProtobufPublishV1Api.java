package orz.springboot.mq.api.pub;

import orz.springboot.kafka.OrzKafkaProtobufPub;
import orz.springboot.mq.annotation.OrzPubApi;
import orz.springboot.mq.api.model.TestProtobufV1To;
import orz.springboot.mq.api.model.TestSchemaProtobufEventBo;

import java.util.concurrent.CompletableFuture;

@OrzPubApi(topic = "test-schema-protobuf")
public class TestSchemaProtobufPublishV1Api extends OrzKafkaProtobufPub<TestSchemaProtobufEventBo, TestProtobufV1To> {
    @Override
    protected CompletableFuture<Void> publish(TestSchemaProtobufEventBo event) {
        return publishMessage(event.toTestProtobufV1To(), extraKey(event.getLongField()));
    }
}
