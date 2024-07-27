package orz.springboot.mq.api.pub;

import orz.springboot.kafka.OrzKafkaProtobufPub;
import orz.springboot.mq.annotation.OrzPubApi;
import orz.springboot.mq.api.model.TestProtobufEventBo;
import orz.springboot.mq.api.model.TestProtobufV1To;

import java.util.concurrent.CompletableFuture;

@OrzPubApi(topic = "test-protobuf")
public class TestProtobufPublishV1Api extends OrzKafkaProtobufPub<TestProtobufEventBo, TestProtobufV1To> {
    @Override
    protected CompletableFuture<Void> publish(TestProtobufEventBo event) {
        return publishMessage(event.toTestProtobufV1To(), extraKey(event.getLongField()));
    }
}
