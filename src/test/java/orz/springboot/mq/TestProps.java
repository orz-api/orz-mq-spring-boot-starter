package orz.springboot.mq;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;

@Data
@Validated
@Component("testProps")
@ConfigurationProperties(prefix = "test")
public class TestProps {
    private boolean enableTestContainer = false;
}
