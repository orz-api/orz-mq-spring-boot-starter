package orz.springboot.mq;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.TestContext;
import org.springframework.test.context.TestExecutionListener;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import javax.annotation.Nonnull;

@Slf4j
@Configuration
public class TestConfiguration implements TestExecutionListener, Ordered {
    @Bean
    public Network containerNetwork() {
        return Network.newNetwork();
    }

    @Bean
    @ServiceConnection
    public KafkaContainer kafkaContainer(Network containerNetwork) {
        var container = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.7.0"));
        container.withNetwork(containerNetwork)
                .withNetworkAliases("kafka");
        return container;
    }

    @Bean
    public GenericContainer<?> schemaRegistryContainer(Network containerNetwork, KafkaContainer kafkaContainer, DynamicPropertyRegistry registry) {
        var container = new GenericContainer<>(DockerImageName.parse("confluentinc/cp-schema-registry:7.7.0"));
        container.withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", String.format("PLAINTEXT://%s:%s", "kafka", 9092))
                .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
                .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
                .withExposedPorts(8081)
                .withNetwork(containerNetwork)
                .withNetworkAliases("schema-registry")
                .dependsOn(kafkaContainer)
                .waitingFor(Wait.forHttp("/subjects").forStatusCode(200));
        registry.add("orz.kafka.pub.TestSchemaJsonPublishV1Api.schemaRegistryUrl", () -> "http://%s:%d".formatted(container.getHost(), container.getMappedPort(8081)));
        registry.add("orz.kafka.sub.TestSchemaJsonSubscribeV1Api.schemaRegistryUrl", () -> "http://%s:%d".formatted(container.getHost(), container.getMappedPort(8081)));
        registry.add("orz.kafka.pub.TestSchemaProtobufPublishV1Api.schemaRegistryUrl", () -> "http://%s:%d".formatted(container.getHost(), container.getMappedPort(8081)));
        registry.add("orz.kafka.sub.TestSchemaProtobufSubscribeV1Api.schemaRegistryUrl", () -> "http://%s:%d".formatted(container.getHost(), container.getMappedPort(8081)));
        return container;
    }

    @Override
    public void beforeTestClass(@Nonnull TestContext testContext) throws Exception {
        System.setProperty("spring.profiles.active", "test");
        TestExecutionListener.super.beforeTestClass(testContext);
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }
}
