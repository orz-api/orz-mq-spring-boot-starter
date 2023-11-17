package orz.springboot.kafka.apollo;

import com.ctrip.framework.apollo.model.ConfigChangeEvent;
import com.ctrip.framework.apollo.spring.annotation.ApolloConfigChangeListener;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import orz.springboot.mq.OrzMq;

import java.util.regex.Pattern;

import static orz.springboot.base.OrzBaseUtils.message;

@Slf4j
@Component
@ConditionalOnClass({KafkaTemplate.class, ApolloConfigChangeListener.class})
public class OrzKafkaApolloChangeListener {
    private final static Pattern SUB_RUNNING_PATTERN = Pattern.compile("^orz\\.kafka\\.sub\\.(?<id>[^.]+)\\.running$");

    private final OrzMq mq;

    public OrzKafkaApolloChangeListener(OrzMq mq) {
        this.mq = mq;
    }

    @ApolloConfigChangeListener(interestedKeyPrefixes = "orz.kafka.sub.")
    private void onSubRunningChanged(ConfigChangeEvent event) {
        for (var key : event.changedKeys()) {
            if (log.isDebugEnabled()) {
                log.debug(message("apollo config changed", "key", key, "value", event.getChange(key).getNewValue()));
            }
            var matcher = SUB_RUNNING_PATTERN.matcher(key);
            if (matcher.matches()) {
                log.info(message("apollo config sub running changed", "key", key, "value", event.getChange(key).getNewValue()));
                var id = matcher.group("id");
                var running = Boolean.parseBoolean(event.getChange(key).getNewValue());
                if (running) {
                    mq.startSub(id);
                } else {
                    mq.stopSub(id);
                }
            }
        }
    }
}
