package orz.springboot.mq;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static orz.springboot.base.OrzBaseUtils.assertion;
import static orz.springboot.base.description.OrzDescriptionUtils.desc;

@Component
public class OrzMqManager {
    private final Map<String, OrzMqSub<?, ?>> subMap = new HashMap<>();
    private final Map<Class<?>, OrzMqPub<?>> pubMap = new HashMap<>();

    public void registerSub(OrzMqSub<?, ?> sub) {
        assertion(sub != null, "sub != null");
        if (subMap.containsKey(sub.getId())) {
            throw new RuntimeException(desc("sub already exists", "id", sub.getId()));
        }
        subMap.put(sub.getId(), sub);
    }

    public void registerPub(OrzMqPub<?> pub) {
        assertion(pub != null, "pub != null");
        if (pubMap.containsKey(pub.getEventType())) {
            throw new RuntimeException(desc("pub already exists", "eventType", pub.getEventType()));
        }
        pubMap.put(pub.getEventType(), pub);
    }

    public void startSub(String id) {
        assertion(StringUtils.isNotBlank(id), "StringUtils.isNotBlank(id)");
        var sub = subMap.get(id);
        if (sub == null) {
            throw new RuntimeException(desc("sub not found", "id", id));
        }
        sub.start();
    }

    public void stopSub(String id) {
        assertion(StringUtils.isNotBlank(id), "StringUtils.isNotBlank(id)");
        var sub = subMap.get(id);
        if (sub == null) {
            throw new RuntimeException(desc("sub not found", "id", id));
        }
        sub.stop();
    }

    public <E> void publish(E event) {
        publishAsync(event).join();
    }

    public <E> CompletableFuture<Void> publishAsync(E event) {
        assertion(event != null, "event != null");
        // noinspection unchecked
        var pub = (OrzMqPub<E>) pubMap.get(event.getClass());
        if (pub == null) {
            throw new RuntimeException(desc("pub not found", "eventType", event.getClass()));
        }
        return pub.publish(event);
    }
}
