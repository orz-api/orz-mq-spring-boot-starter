package orz.springboot.mq;

import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import orz.springboot.mq.api.model.TestJsonEventBo;
import orz.springboot.mq.api.model.TestProtobufEventBo;
import orz.springboot.mq.api.model.TestJsonSchemaEventBo;
import orz.springboot.mq.api.model.TestProtobufSchemaEventBo;
import orz.springboot.mq.api.sub.*;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class AppTests {
    @Autowired
    private OrzMqPublisher<TestJsonEventBo> testJsonPublisher;

    @Autowired
    private OrzMqPublisher<TestJsonSchemaEventBo> testSchemaJsonPublisher;

    @Autowired
    private OrzMqPublisher<TestProtobufEventBo> testProtobufPublisher;

    @Autowired
    private OrzMqPublisher<TestProtobufSchemaEventBo> testSchemaProtobufPublisher;

    @Autowired
    private TestJsonSubscribeV1Api testJsonSubscribeV1Api;

    @Autowired
    private TestJsonSchemaSubscribeV1Api testJsonSchemaSubscribeV1Api;

    @Autowired
    private TestProtobufSubscribeV1Api testProtobufSubscribeV1Api;

    @Autowired
    private TestProtobufSchemaSubscribeV1Api testProtobufSchemaSubscribeV1Api;

    @Autowired
    private TestJsonDltSubscribeV1Api testJsonDltSubscribeV1Api;

    @Autowired
    private TestJsonSchemaDltSubscribeV1Api testJsonSchemaDltSubscribeV1Api;

    @Autowired
    private TestProtobufDltSubscribeV1Api testProtobufDltSubscribeV1Api;

    @Autowired
    private TestProtobufSchemaDltSubscribeV1Api testProtobufSchemaDltSubscribeV1Api;

    @Test
    @SneakyThrows
    void testJson() {
        // full message
        {
            var event = new TestJsonEventBo(
                    (byte) 0, new byte[]{Byte.MIN_VALUE, Byte.MAX_VALUE},
                    (short) 0, new short[]{Short.MIN_VALUE, Short.MAX_VALUE},
                    0, new int[]{Integer.MIN_VALUE, Integer.MAX_VALUE},
                    0L, new long[]{Long.MIN_VALUE, Long.MAX_VALUE},
                    (float) 0, new float[]{Float.MIN_VALUE, Float.MAX_VALUE},
                    0, new double[]{Double.MIN_VALUE, Double.MAX_VALUE},
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
            testJsonPublisher.publish(event);
            var message = testJsonSubscribeV1Api.takeLastMessage();
            assertNotNull(message);
            assertEquals(event.toTestJsonV1To(), message);
            var key = testJsonSubscribeV1Api.takeLastKey();
            assertNotNull(key);
        }

        // empty message
        {
            var event = new TestJsonEventBo();
            testJsonPublisher.publish(event);
            var message = testJsonSubscribeV1Api.takeLastMessage();
            assertNotNull(message);
            assertEquals(event.toTestJsonV1To(), message);
            var key = testJsonSubscribeV1Api.takeLastKey();
            assertNull(key);
        }

        // dlt
        {
            var event = new TestJsonEventBo();
            event.setStrField("TestDlt");
            testJsonPublisher.publish(event);
            var message = testJsonSubscribeV1Api.takeLastMessage();
            assertNotNull(message);
            assertEquals(event.toTestJsonV1To(), message);
            var key = testJsonSubscribeV1Api.takeLastKey();
            assertNull(key);
            var dltMessage = testJsonDltSubscribeV1Api.takeLastMessage();
            assertNotNull(dltMessage);
            assertEquals(event.toTestJsonV1To(), dltMessage);
            var dltKey = testJsonDltSubscribeV1Api.takeLastKey();
            assertNull(dltKey);
        }
    }

    @Test
    @SneakyThrows
    void testSchemaJson() {
        // full message
        {
            var event = new TestJsonSchemaEventBo(
                    (byte) 0, new byte[]{Byte.MIN_VALUE, Byte.MAX_VALUE},
                    (short) 0, new short[]{Short.MIN_VALUE, Short.MAX_VALUE},
                    0, new int[]{Integer.MIN_VALUE, Integer.MAX_VALUE},
                    0L, new long[]{Long.MIN_VALUE, Long.MAX_VALUE},
                    (float) 0, new float[]{Float.MIN_VALUE, Float.MAX_VALUE},
                    0, new double[]{Double.MIN_VALUE, Double.MAX_VALUE},
                    'a', new char[]{'1', '2'},
                    (byte) 0, new Byte[]{Byte.MIN_VALUE, Byte.MAX_VALUE}, List.of(Byte.MIN_VALUE, Byte.MAX_VALUE),
                    (short) 0, new Short[]{Short.MIN_VALUE, Short.MAX_VALUE}, List.of(Short.MIN_VALUE, Short.MAX_VALUE),
                    0, new Integer[]{Integer.MIN_VALUE, Integer.MAX_VALUE}, List.of(Integer.MIN_VALUE, Integer.MAX_VALUE),
                    0L, new Long[]{Long.MIN_VALUE, Long.MAX_VALUE}, List.of(Long.MIN_VALUE, Long.MAX_VALUE),
                    (float) 0, new Float[]{Float.MIN_VALUE, Float.MAX_VALUE}, List.of(Float.MIN_VALUE, Float.MAX_VALUE),
                    (double) 0, new Double[]{Double.MIN_VALUE, Double.MAX_VALUE}, List.of(Double.MIN_VALUE, Double.MAX_VALUE),
                    'a', new Character[]{'1', '2'}, List.of('1', '2'),
                    "str", new String[]{"a1", "a2"}, List.of("l1", "l2"),
                    new TestJsonSchemaEventBo.InnerObjectBo("inner"),
                    new TestJsonSchemaEventBo.InnerObjectBo[]{
                            new TestJsonSchemaEventBo.InnerObjectBo("inner a1"),
                            new TestJsonSchemaEventBo.InnerObjectBo("inner a2")
                    },
                    List.of(
                            new TestJsonSchemaEventBo.InnerObjectBo("inner l1"),
                            new TestJsonSchemaEventBo.InnerObjectBo("inner l2")
                    ),
                    LocalDateTime.now(), LocalDate.now()
            );
            testSchemaJsonPublisher.publish(event);
            var message = testJsonSchemaSubscribeV1Api.takeLastMessage();
            assertNotNull(message);
            assertEquals(event.toTestJsonV1To(), message);
            var key = testJsonSchemaSubscribeV1Api.takeLastKey();
            assertNotNull(key);
            assertEquals(key, String.valueOf(event.getLongObjField()));
        }

        // empty message
        {
            var event = new TestJsonSchemaEventBo();
            testSchemaJsonPublisher.publish(event);
            var message = testJsonSchemaSubscribeV1Api.takeLastMessage();
            assertNotNull(message);
            assertEquals(event.toTestJsonV1To(), message);
            var key = testJsonSchemaSubscribeV1Api.takeLastKey();
            assertNull(key);
        }

        // dlt
        {
            var event = new TestJsonSchemaEventBo();
            event.setStrField("TestDlt");
            testSchemaJsonPublisher.publish(event);
            var message = testJsonSchemaSubscribeV1Api.takeLastMessage();
            assertNotNull(message);
            assertEquals(event.toTestJsonV1To(), message);
            var key = testJsonSchemaSubscribeV1Api.takeLastKey();
            assertNull(key);
            var dltMessage = testJsonSchemaDltSubscribeV1Api.takeLastMessage();
            assertNotNull(dltMessage);
            assertEquals(event.toTestJsonV1To(), dltMessage);
            var dltKey = testJsonSchemaDltSubscribeV1Api.takeLastKey();
            assertNull(dltKey);
        }
    }

    @Test
    @SneakyThrows
    void testProtobuf() {
        // full message
        {
            var event = new TestProtobufEventBo(
                    new byte[]{Byte.MIN_VALUE, Byte.MAX_VALUE},
                    0, List.of(Integer.MIN_VALUE, Integer.MAX_VALUE),
                    0L, List.of(Long.MIN_VALUE, Long.MAX_VALUE),
                    (float) 0, List.of(Float.MIN_VALUE, Float.MAX_VALUE),
                    (double) 0, List.of(Double.MIN_VALUE, Double.MAX_VALUE),
                    "str", List.of("l1", "l2"),
                    new TestProtobufEventBo.InnerObjectBo("inner"),
                    List.of(
                            new TestProtobufEventBo.InnerObjectBo("inner l1"),
                            new TestProtobufEventBo.InnerObjectBo("inner l2")
                    ),
                    LocalDateTime.now(), LocalDate.now()
            );
            testProtobufPublisher.publish(event);
            var message = testProtobufSubscribeV1Api.takeLastMessage();
            assertNotNull(message);
            assertEquals(event.toTestProtobufV1To(), message);
            var key = testProtobufSubscribeV1Api.takeLastKey();
            assertNotNull(key);
            assertEquals(key, String.valueOf(event.getLongField()));
        }

        // empty message
        {
            var event = new TestProtobufEventBo();
            testProtobufPublisher.publish(event);
            var message = testProtobufSubscribeV1Api.takeLastMessage();
            assertNotNull(message);
            assertEquals(event.toTestProtobufV1To(), message);
            var key = testProtobufSubscribeV1Api.takeLastKey();
            assertNull(key);
        }

        // dlt
        {
            var event = new TestProtobufEventBo();
            event.setStrField("TestDlt");
            testProtobufPublisher.publish(event);
            var message = testProtobufSubscribeV1Api.takeLastMessage();
            assertNotNull(message);
            assertEquals(event.toTestProtobufV1To(), message);
            var key = testProtobufSubscribeV1Api.takeLastKey();
            assertNull(key);
            var dltMessage = testProtobufDltSubscribeV1Api.takeLastMessage();
            assertNotNull(dltMessage);
            assertEquals(event.toTestProtobufV1To(), dltMessage);
            var dltKey = testProtobufDltSubscribeV1Api.takeLastKey();
            assertNull(dltKey);
        }
    }

    @Test
    @SneakyThrows
    void testSchemaProtobuf() {
        // full message
        {
            var event = new TestProtobufSchemaEventBo(
                    new byte[]{Byte.MIN_VALUE, Byte.MAX_VALUE},
                    0, List.of(Integer.MIN_VALUE, Integer.MAX_VALUE),
                    0L, List.of(Long.MIN_VALUE, Long.MAX_VALUE),
                    (float) 0, List.of(Float.MIN_VALUE, Float.MAX_VALUE),
                    (double) 0, List.of(Double.MIN_VALUE, Double.MAX_VALUE),
                    "str", List.of("l1", "l2"),
                    new TestProtobufSchemaEventBo.InnerObjectBo("inner"),
                    List.of(
                            new TestProtobufSchemaEventBo.InnerObjectBo("inner l1"),
                            new TestProtobufSchemaEventBo.InnerObjectBo("inner l2")
                    ),
                    LocalDateTime.now(), LocalDate.now()
            );
            testSchemaProtobufPublisher.publish(event);
            var message = testProtobufSchemaSubscribeV1Api.takeLastMessage();
            assertNotNull(message);
            assertEquals(event.toTestProtobufV1To(), message);
            var key = testProtobufSchemaSubscribeV1Api.takeLastKey();
            assertNotNull(key);
            assertEquals(key, String.valueOf(event.getLongField()));
        }

        // empty message
        {
            var event = new TestProtobufSchemaEventBo();
            testSchemaProtobufPublisher.publish(event);
            var message = testProtobufSchemaSubscribeV1Api.takeLastMessage();
            assertNotNull(message);
            assertEquals(event.toTestProtobufV1To(), message);
            var key = testProtobufSchemaSubscribeV1Api.takeLastKey();
            assertNull(key);
        }

        // dlt
        {
            var event = new TestProtobufSchemaEventBo();
            event.setStrField("TestDlt");
            testSchemaProtobufPublisher.publish(event);
            var message = testProtobufSchemaSubscribeV1Api.takeLastMessage();
            assertNotNull(message);
            assertEquals(event.toTestProtobufV1To(), message);
            var key = testProtobufSchemaSubscribeV1Api.takeLastKey();
            assertNull(key);
            var dltMessage = testProtobufSchemaDltSubscribeV1Api.takeLastMessage();
            assertNotNull(dltMessage);
            assertEquals(event.toTestProtobufV1To(), dltMessage);
            var dltKey = testProtobufSchemaDltSubscribeV1Api.takeLastKey();
            assertNull(dltKey);
        }
    }
}
