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
    private final Map<String, OrzMqSub<?, ?>> subMap = new HashMap<>();
    private final Map<String, OrzMqSub<?, ?>> fullQualifierSubMap = new HashMap<>();
    private final Map<Class<?>, OrzMqPub<?>> pubMap = new HashMap<>();

    public synchronized void registerSub(OrzMqSub<?, ?> sub) {
        assertion(sub != null, "sub != null");
        var exists = subMap.get(sub.getId());
        if (exists != null && exists != sub) {
            throw new FatalBeanException(desc("sub already exists", "id", sub.getId(), "sub", sub.getClass().getName(), "exists", exists.getClass().getName()));
        }
        var fullQualifierExists = fullQualifierSubMap.get(sub.getFullQualifier());
        if (fullQualifierExists != null && fullQualifierExists != sub) {
            throw new FatalBeanException(desc("sub already exists", "fullQualifier", sub.getFullQualifier(), "sub", sub.getClass().getName(), "exists", fullQualifierExists.getClass().getName()));
        }
        subMap.put(sub.getId(), sub);
        fullQualifierSubMap.put(sub.getFullQualifier(), sub);
    }

    public synchronized void registerPub(OrzMqPub<?> pub) {
        assertion(pub != null, "pub != null");
        var exists = pubMap.get(pub.getDataType());
        if (exists == pub) {
            return;
        }
        if (exists != null) {
            throw new FatalBeanException(desc("pub already exists", "dataType", pub.getDataType().getName(), "pub", pub.getClass().getName(), "exists", exists.getClass().getName()));
        }
        pubMap.put(pub.getDataType(), pub);
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

    public <E> void publish(E data) throws Exception {
        publishAsync(data).get();
    }

    public <D> CompletableFuture<Void> publishAsync(D data) {
        assertion(data != null, "data != null");
        // noinspection unchecked
        var pub = (OrzMqPub<D>) pubMap.get(data.getClass());
        if (pub == null) {
            throw new RuntimeException(desc("pub not found", "dataType", data.getClass()));
        }
        return pub.publish(data);
    }
}
