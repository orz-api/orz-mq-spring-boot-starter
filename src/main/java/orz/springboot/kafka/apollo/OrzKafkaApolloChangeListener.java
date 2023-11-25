package orz.springboot.kafka.apollo;

import com.ctrip.framework.apollo.model.ConfigChangeEvent;
import com.ctrip.framework.apollo.spring.annotation.ApolloConfigChangeListener;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import orz.springboot.mq.OrzMqManager;

import java.util.regex.Pattern;

import static orz.springboot.base.description.OrzDescriptionUtils.desc;

@Slf4j
@Component
@ConditionalOnClass({KafkaTemplate.class, ApolloConfigChangeListener.class})
public class OrzKafkaApolloChangeListener {
    private final static Pattern SUB_RUNNING_PATTERN = Pattern.compile("^orz\\.kafka\\.sub\\.(?<id>[^.]+)\\.running$");

    private final OrzMqManager mqManager;

    public OrzKafkaApolloChangeListener(OrzMqManager mqManager) {
        this.mqManager = mqManager;
    }

    @ApolloConfigChangeListener(interestedKeyPrefixes = "orz.kafka.sub.")
    private void onSubRunningChanged(ConfigChangeEvent event) {
        for (var key : event.changedKeys()) {
            if (log.isDebugEnabled()) {
                log.debug(desc("apollo config changed", "key", key, "value", event.getChange(key).getNewValue()));
            }
            var matcher = SUB_RUNNING_PATTERN.matcher(key);
            if (matcher.matches()) {
                log.info(desc("apollo config kafka sub running changed", "key", key, "value", event.getChange(key).getNewValue()));
                var id = matcher.group("id");
                var running = Boolean.parseBoolean(event.getChange(key).getNewValue());
                if (running) {
                    mqManager.startSub(id);
                } else {
                    mqManager.stopSub(id);
                }
            }
        }
    }
}
