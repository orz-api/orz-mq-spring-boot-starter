package orz.springboot.mq;

import orz.springboot.base.OrzBaseUtils;

public class OrzMqUtils {
    public static <T> Class<T> getSubMessageType(Class<?> cls) {
        return OrzBaseUtils.getClassGenericParameter(cls);
    }

    public static <T> Class<T> getPubEventType(Class<?> cls) {
        return OrzBaseUtils.getClassGenericParameter(cls);
    }

    public static <T> Class<T> getPubMessageType(Class<?> cls) {
        return OrzBaseUtils.getClassGenericParameter(cls, 1);
    }
}
