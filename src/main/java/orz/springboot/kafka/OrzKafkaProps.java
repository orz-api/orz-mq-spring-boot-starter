package orz.springboot.kafka;

import jakarta.annotation.Nullable;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;
import org.springframework.validation.annotation.Validated;
import orz.springboot.alarm.exception.OrzUnexpectedException;
import orz.springboot.kafka.model.OrzKafkaSubRunningChangeEventBo;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static orz.springboot.base.description.OrzDescriptionUtils.desc;

@Slf4j
@Data
@Validated
@ConfigurationProperties(prefix = "orz.kafka")
@ConditionalOnClass({KafkaTemplate.class})
public class OrzKafkaProps {
    @Valid
    private Map<String, SubConfig> sub = Collections.emptyMap();

    @Valid
    private Map<String, PubConfig> pub = Collections.emptyMap();

    @Valid
    private Map<String, SchemaRegistryConfig> schemaRegistry = Collections.emptyMap();

    @Valid
    private BackOffConfig backOff = new BackOffConfig();

    @EventListener
    public void onKafkaSubRunningChange(OrzKafkaSubRunningChangeEventBo event) {
        var id = event.getSub().getId();
        var oldConfig = sub.get(id);
        log.info(desc("kafka sub running changed", "id", id, "running", event.isRunning(), "oldConfig", oldConfig));
        var newConfig = Optional.ofNullable(oldConfig).map(SubConfig::new).orElseGet(SubConfig::new);
        newConfig.setRunning(event.isRunning());
        var map = new HashMap<>(sub);
        map.put(id, newConfig);
        sub = Collections.unmodifiableMap(map);
    }

    @Nullable
    public SubConfig getSub(String id) {
        return sub.get(id);
    }

    @Nullable
    public PubConfig getPub(String id) {
        return pub.get(id);
    }

    @Nullable
    public PubConfig getSubDltPub(String id) {
        var sub = getSub(id);
        if (sub != null) {
            var dltPub = new PubConfig(sub.getDltPub());
            if (StringUtils.isBlank(dltPub.getBootstrapServers())) {
                dltPub.setBootstrapServers(sub.getBootstrapServers());
            }
            if (StringUtils.isBlank(dltPub.getSchemaRegistry())) {
                dltPub.setSchemaRegistry(sub.getSchemaRegistry());
            }
            return dltPub;
        }
        return null;
    }

    @Nullable
    public SchemaRegistryConfig getSubSchemaRegistry(String id) {
        return Optional.ofNullable(getSub(id))
                .map(SubConfig::getSchemaRegistry)
                .map(this::getSchemaRegistry)
                .orElse(null);
    }

    @Nullable
    public SchemaRegistryConfig getPubSchemaRegistry(String id) {
        return Optional.ofNullable(getPub(id))
                .map(PubConfig::getSchemaRegistry)
                .map(this::getSchemaRegistry)
                .orElse(null);
    }

    @Nullable
    public SchemaRegistryConfig getSubDltPubSchemaRegistry(String id) {
        return Optional.ofNullable(getSubDltPub(id))
                .map(PubConfig::getSchemaRegistry)
                .map(this::getSchemaRegistry)
                .orElse(null);
    }

    @Nullable
    public SchemaRegistryConfig getSchemaRegistry(String id) {
        if (StringUtils.isBlank(id)) {
            return null;
        }
        var registry = schemaRegistry.get(id);
        if (registry == null) {
            throw new OrzUnexpectedException("OrzKafkaProps schema registry not found", "id", id);
        }
        return schemaRegistry.get(id);
    }

    public BackOff getSubBackOff(String id) {
        return Optional.ofNullable(getSub(id)).map(SubConfig::getBackOff).orElse(backOff).toInstance();
    }

    @Data
    public static class SubConfig {
        private boolean running = true;

        @Positive
        private Integer concurrency = null;

        private String bootstrapServers = null;

        private String schemaRegistry = null;

        @Valid
        @NotNull
        private PubConfig dltPub = new PubConfig();

        @Valid
        private BackOffConfig backOff = null;

        public SubConfig() {
        }

        public SubConfig(SubConfig other) {
            this.running = other.running;
            this.concurrency = other.concurrency;
            this.bootstrapServers = other.bootstrapServers;
            this.schemaRegistry = other.schemaRegistry;
            this.dltPub = other.dltPub;
            this.backOff = other.backOff;
        }
    }

    @Data
    public static class PubConfig {
        private String bootstrapServers = null;

        private String schemaRegistry = null;

        public PubConfig() {
        }

        public PubConfig(PubConfig other) {
            this.bootstrapServers = other.bootstrapServers;
            this.schemaRegistry = other.schemaRegistry;
        }
    }

    @Data
    public static class SchemaRegistryConfig {
        @NotBlank
        private String url = null;
    }

    @Data
    public static class BackOffConfig {
        @NotNull
        private BackOffType type = BackOffType.FIXED;

        @Valid
        private BackOffFixedConfig fixed = new BackOffFixedConfig();

        @Valid
        private BackOffExponentialConfig exponential = new BackOffExponentialConfig();

        @Valid
        private BackOffExponentialWithMaxRetiresConfig exponentialWithMaxRetires = new BackOffExponentialWithMaxRetiresConfig();

        public BackOff toInstance() {
            return switch (type) {
                case FIXED -> fixed.toInstance();
                case EXPONENTIAL -> exponential.toInstance();
                case EXPONENTIAL_WITH_MAX_RETIRES -> exponentialWithMaxRetires.toInstance();
            };
        }
    }

    public enum BackOffType {
        FIXED,
        EXPONENTIAL,
        EXPONENTIAL_WITH_MAX_RETIRES
    }

    @Data
    public static class BackOffFixedConfig {
        @Positive
        private long interval = 1000L;

        @Positive
        private long maxAttempts = 5L;

        public BackOff toInstance() {
            return new FixedBackOff(interval, maxAttempts);
        }
    }

    @Data
    public static class BackOffExponentialConfig {
        @Positive
        private long initialInterval = 1000L;

        @Positive
        private double multiplier = 1.5;

        @Positive
        private long maxInterval = 5000L;

        @Positive
        private long maxElapsedTime = 15000L;

        public BackOff toInstance() {
            var backOff = new ExponentialBackOff(initialInterval, multiplier);
            backOff.setMaxInterval(maxInterval);
            backOff.setMaxElapsedTime(maxElapsedTime);
            return backOff;
        }
    }

    @Data
    public static class BackOffExponentialWithMaxRetiresConfig {
        @Positive
        private long initialInterval = 1000L;

        @Positive
        private double multiplier = 1.5;

        @Positive
        private long maxInterval = 5000L;

        @Positive
        private int maxRetries = 5;

        public BackOff toInstance() {
            var backOff = new ExponentialBackOffWithMaxRetries(maxRetries);
            backOff.setInitialInterval(initialInterval);
            backOff.setMultiplier(multiplier);
            backOff.setMaxInterval(maxInterval);
            return backOff;
        }
    }
}
