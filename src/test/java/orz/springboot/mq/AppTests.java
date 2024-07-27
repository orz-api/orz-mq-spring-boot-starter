package orz.springboot.mq;

import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import orz.springboot.mq.api.model.TestJsonEventBo;
import orz.springboot.mq.api.model.TestSchemaJsonEventBo;
import orz.springboot.mq.api.model.TestSchemaProtobufEventBo;
import orz.springboot.mq.api.sub.TestJsonSubscribeV1Api;
import orz.springboot.mq.api.sub.TestSchemaJsonSubscribeV1Api;
import orz.springboot.mq.api.sub.TestSchemaProtobufSubscribeV1Api;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class AppTests {
    @Autowired
    private OrzMqPublisher<TestJsonEventBo> testJsonPublisher;

    @Autowired
    private OrzMqPublisher<TestSchemaJsonEventBo> testSchemaJsonPublisher;

    @Autowired
    private OrzMqPublisher<TestSchemaProtobufEventBo> testSchemaProtobufPublisher;

    @Autowired
    private TestJsonSubscribeV1Api testJsonSubscribeV1Api;

    @Autowired
    private TestSchemaJsonSubscribeV1Api testSchemaJsonSubscribeV1Api;

    @Autowired
    private TestSchemaProtobufSubscribeV1Api testSchemaProtobufSubscribeV1Api;

    @Test
    @SneakyThrows
    void testJson() {
        var event1 = new TestJsonEventBo(
                (byte) 0, new byte[]{Byte.MIN_VALUE, Byte.MAX_VALUE},
                (short) 0, new short[]{Short.MIN_VALUE, Short.MAX_VALUE},
                0, new int[]{Integer.MIN_VALUE, Integer.MAX_VALUE},
                0L, new long[]{Long.MIN_VALUE, Long.MAX_VALUE},
                (float) 0, new float[]{Float.MIN_VALUE, Float.MAX_VALUE},
                (double) 0, new double[]{Double.MIN_VALUE, Double.MAX_VALUE},
                'a', new char[]{'1', '2'},
                (byte) 0, new Byte[]{Byte.MIN_VALUE, Byte.MAX_VALUE}, List.of(Byte.MIN_VALUE, Byte.MAX_VALUE),
                (short) 0, new Short[]{Short.MIN_VALUE, Short.MAX_VALUE}, List.of(Short.MIN_VALUE, Short.MAX_VALUE),
                0, new Integer[]{Integer.MIN_VALUE, Integer.MAX_VALUE}, List.of(Integer.MIN_VALUE, Integer.MAX_VALUE),
                0L, new Long[]{Long.MIN_VALUE, Long.MAX_VALUE}, List.of(Long.MIN_VALUE, Long.MAX_VALUE),
                (float) 0, new Float[]{Float.MIN_VALUE, Float.MAX_VALUE}, List.of(Float.MIN_VALUE, Float.MAX_VALUE),
                (double) 0, new Double[]{Double.MIN_VALUE, Double.MAX_VALUE}, List.of(Double.MIN_VALUE, Double.MAX_VALUE),
                'a', new Character[]{'1', '2'}, List.of('1', '2'),
                "str", new String[]{"a1", "a2"}, List.of("l1", "l2"),
                new TestJsonEventBo.InnerObjectBo("inner"),
                new TestJsonEventBo.InnerObjectBo[]{
                        new TestJsonEventBo.InnerObjectBo("inner a1"),
                        new TestJsonEventBo.InnerObjectBo("inner a2")
                },
                List.of(
                        new TestJsonEventBo.InnerObjectBo("inner l1"),
                        new TestJsonEventBo.InnerObjectBo("inner l2")
                ),
                LocalDateTime.now(), LocalDate.now()
        );
        testJsonPublisher.publish(event1);
        var message1 = testJsonSubscribeV1Api.takeLastMessage();
        assertNotNull(message1);
        assertEquals(event1.toTestJsonV1To(), message1);
        var key1 = testJsonSubscribeV1Api.takeLastKey();
        assertNotNull(key1);

        var event2 = new TestJsonEventBo();
        testJsonPublisher.publish(event2);
        var message2 = testJsonSubscribeV1Api.takeLastMessage();
        assertNotNull(message2);
        assertEquals(event2.toTestJsonV1To(), message2);
        var key2 = testJsonSubscribeV1Api.takeLastKey();
        assertNull(key2);
    }

    @Test
    @SneakyThrows
    void testSchemaJson() {
        var event1 = new TestSchemaJsonEventBo(
                (byte) 0, new byte[]{Byte.MIN_VALUE, Byte.MAX_VALUE},
                (short) 0, new short[]{Short.MIN_VALUE, Short.MAX_VALUE},
                0, new int[]{Integer.MIN_VALUE, Integer.MAX_VALUE},
                0L, new long[]{Long.MIN_VALUE, Long.MAX_VALUE},
                (float) 0, new float[]{Float.MIN_VALUE, Float.MAX_VALUE},
                (double) 0, new double[]{Double.MIN_VALUE, Double.MAX_VALUE},
                'a', new char[]{'1', '2'},
                (byte) 0, new Byte[]{Byte.MIN_VALUE, Byte.MAX_VALUE}, List.of(Byte.MIN_VALUE, Byte.MAX_VALUE),
                (short) 0, new Short[]{Short.MIN_VALUE, Short.MAX_VALUE}, List.of(Short.MIN_VALUE, Short.MAX_VALUE),
                0, new Integer[]{Integer.MIN_VALUE, Integer.MAX_VALUE}, List.of(Integer.MIN_VALUE, Integer.MAX_VALUE),
                0L, new Long[]{Long.MIN_VALUE, Long.MAX_VALUE}, List.of(Long.MIN_VALUE, Long.MAX_VALUE),
                (float) 0, new Float[]{Float.MIN_VALUE, Float.MAX_VALUE}, List.of(Float.MIN_VALUE, Float.MAX_VALUE),
                (double) 0, new Double[]{Double.MIN_VALUE, Double.MAX_VALUE}, List.of(Double.MIN_VALUE, Double.MAX_VALUE),
                'a', new Character[]{'1', '2'}, List.of('1', '2'),
                "str", new String[]{"a1", "a2"}, List.of("l1", "l2"),
                new TestSchemaJsonEventBo.InnerObjectBo("inner"),
                new TestSchemaJsonEventBo.InnerObjectBo[]{
                        new TestSchemaJsonEventBo.InnerObjectBo("inner a1"),
                        new TestSchemaJsonEventBo.InnerObjectBo("inner a2")
                },
                List.of(
                        new TestSchemaJsonEventBo.InnerObjectBo("inner l1"),
                        new TestSchemaJsonEventBo.InnerObjectBo("inner l2")
                ),
                LocalDateTime.now(), LocalDate.now()
        );
        testSchemaJsonPublisher.publish(event1);
        var message1 = testSchemaJsonSubscribeV1Api.takeLastMessage();
        assertNotNull(message1);
        assertEquals(event1.toTestJsonV1To(), message1);
        var key1 = testSchemaJsonSubscribeV1Api.takeLastKey();
        assertNotNull(key1);
        assertEquals(key1, String.valueOf(event1.getLongObjField()));

        var event2 = new TestSchemaJsonEventBo();
        testSchemaJsonPublisher.publish(event2);
        var message2 = testSchemaJsonSubscribeV1Api.takeLastMessage();
        assertNotNull(message2);
        assertEquals(event2.toTestJsonV1To(), message2);
        var key2 = testSchemaJsonSubscribeV1Api.takeLastKey();
        assertNull(key2);
    }

    @Test
    @SneakyThrows
    void testSchemaProtobuf() {
        var event1 = new TestSchemaProtobufEventBo(
                new byte[]{Byte.MIN_VALUE, Byte.MAX_VALUE},
                0, List.of(Integer.MIN_VALUE, Integer.MAX_VALUE),
                0L, List.of(Long.MIN_VALUE, Long.MAX_VALUE),
                (float) 0, List.of(Float.MIN_VALUE, Float.MAX_VALUE),
                (double) 0, List.of(Double.MIN_VALUE, Double.MAX_VALUE),
                "str", List.of("l1", "l2"),
                new TestSchemaProtobufEventBo.InnerObjectBo("inner"),
                List.of(
                        new TestSchemaProtobufEventBo.InnerObjectBo("inner l1"),
                        new TestSchemaProtobufEventBo.InnerObjectBo("inner l2")
                ),
                LocalDateTime.now(), LocalDate.now()
        );
        testSchemaProtobufPublisher.publish(event1);
        var message1 = testSchemaProtobufSubscribeV1Api.takeLastMessage();
        assertNotNull(message1);
        assertEquals(event1.toTestProtobufV1To(), message1);
        var key1 = testSchemaProtobufSubscribeV1Api.takeLastKey();
        assertNotNull(key1);
        assertEquals(key1, String.valueOf(event1.getLongField()));

        var event2 = new TestSchemaProtobufEventBo();
        testSchemaProtobufPublisher.publish(event2);
        var message2 = testSchemaProtobufSubscribeV1Api.takeLastMessage();
        assertNotNull(message2);
        assertEquals(event2.toTestProtobufV1To(), message2);
        var key2 = testSchemaProtobufSubscribeV1Api.takeLastKey();
        assertNull(key2);
    }
}
