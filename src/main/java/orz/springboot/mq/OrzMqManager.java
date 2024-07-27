package orz.springboot.mq;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.FatalBeanException;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static orz.springboot.base.OrzBaseUtils.assertion;
import static orz.springboot.base.description.OrzDescriptionUtils.desc;

@Component
public class OrzMqManager {
    private final Map<String, OrzMqSub<?, ?>> subIdMap = new HashMap<>();
    private final Map<String, OrzMqSub<?, ?>> subFullQualifierMap = new HashMap<>();
    private final Map<String, OrzMqPub<?, ?>> pubIdMap = new HashMap<>();
    private final Map<Class<?>, OrzMqPub<?, ?>> pubEventTypeMap = new HashMap<>();

    public synchronized void registerSub(OrzMqSub<?, ?> sub) {
        assertion(sub != null, "sub != null");
        var exists = subIdMap.get(sub.getId());
        if (exists != null && exists != sub) {
            throw new FatalBeanException(desc("sub already exists", "id", sub.getId(), "sub", sub.getClass().getName(), "exists", exists.getClass().getName()));
        }
        var fullQualifierExists = subFullQualifierMap.get(sub.getFullQualifier());
        if (fullQualifierExists != null && fullQualifierExists != sub) {
            throw new FatalBeanException(desc("sub already exists", "fullQualifier", sub.getFullQualifier(), "sub", sub.getClass().getName(), "exists", fullQualifierExists.getClass().getName()));
        }
        subIdMap.put(sub.getId(), sub);
        subFullQualifierMap.put(sub.getFullQualifier(), sub);
    }

    public synchronized void registerPub(OrzMqPub<?, ?> pub) {
        assertion(pub != null, "pub != null");
        var exists = pubIdMap.get(pub.getId());
        if (exists != null && exists != pub) {
            throw new FatalBeanException(desc("pub already exists", "id", pub.getId(), "pub", pub.getClass().getName(), "exists", exists.getClass().getName()));
        }
        var eventTypeExists = pubEventTypeMap.get(pub.getEventType());
        if (eventTypeExists != null && eventTypeExists != pub) {
            throw new FatalBeanException(desc("pub already exists", "eventType", pub.getEventType().getName(), "pub", pub.getClass().getName(), "exists", eventTypeExists.getClass().getName()));
        }
        pubIdMap.put(pub.getId(), pub);
        pubEventTypeMap.put(pub.getEventType(), pub);
    }

    public void startSub(String id) {
        assertion(StringUtils.isNotBlank(id), "StringUtils.isNotBlank(id)");
        var sub = subIdMap.get(id);
        if (sub == null) {
            throw new RuntimeException(desc("sub not found", "id", id));
        }
        sub.start();
    }

    public void stopSub(String id) {
        assertion(StringUtils.isNotBlank(id), "StringUtils.isNotBlank(id)");
        var sub = subIdMap.get(id);
        if (sub == null) {
            throw new RuntimeException(desc("sub not found", "id", id));
        }
        sub.stop();
    }

    public <E> void publish(E event) throws Exception {
        publishAsync(event).get();
    }

    public <E> CompletableFuture<Void> publishAsync(E event) {
        assertion(event != null, "event != null");
        // noinspection unchecked
        var pub = (OrzMqPub<E, ?>) pubEventTypeMap.get(event.getClass());
        if (pub == null) {
            throw new RuntimeException(desc("pub not found", "eventType", event.getClass()));
        }
        return pub.publish(event);
    }
}
