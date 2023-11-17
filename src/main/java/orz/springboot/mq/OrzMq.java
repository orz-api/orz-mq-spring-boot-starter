package orz.springboot.mq;

import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static orz.springboot.base.OrzBaseUtils.message;

@Component
public class OrzMq {
    private final Map<String, OrzSubBase<?, ?>> subMap = new HashMap<>();
    private final Map<Class<?>, OrzPubBase<?>> pubMap = new HashMap<>();

    public void registerSub(OrzSubBase<?, ?> sub) {
        Assert.notNull(sub, "sub is null");
        Assert.isTrue(!subMap.containsKey(sub.getId()), message("sub already exists", "id", sub.getId()));
        subMap.put(sub.getId(), sub);
    }

    public void registerPub(OrzPubBase<?> pub) {
        Assert.notNull(pub, "pub is null");
        Assert.isTrue(!pubMap.containsKey(pub.getEventType()), message("pub already exists", "eventType", pub.getEventType()));
        pubMap.put(pub.getEventType(), pub);
    }

    public void startSub(String id) {
        Assert.hasText(id, "id is empty");
        var sub = subMap.get(id);
        sub.start();
    }

    public void stopSub(String id) {
        Assert.hasText(id, "id is empty");
        var sub = subMap.get(id);
        sub.stop();
    }

    public <E> void publish(E event) {
        publishAsync(event).join();
    }

    public <E> CompletableFuture<Void> publishAsync(E event) {
        Assert.notNull(event, "event is null");
        // noinspection unchecked
        var pub = (OrzPubBase<E>) pubMap.get(event.getClass());
        Assert.notNull(pub, message("pub not found", "eventType", event.getClass()));
        return pub.onPublish(event);
    }
}
