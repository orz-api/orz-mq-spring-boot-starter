package orz.springboot.mq;

import orz.springboot.base.OrzBaseUtils;

public class OrzMqUtils {
    public static <T> Class<T> getSubDataType(Class<?> cls) {
        return OrzBaseUtils.getClassGenericParameter(cls);
    }

    public static <T> Class<T> getPubDataType(Class<?> cls) {
        return OrzBaseUtils.getClassGenericParameter(cls);
    }
}
