package orz.springboot.mq;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.springframework.core.convert.converter.Converter;

import javax.annotation.Nonnull;
import java.util.Map;

public class OrzMqSubConverters {
    private static final Map<Class<?>, Converter<String, ?>> STRING_CONVERTER_MAP = Map.of(
            String.class, new StringToStringConverter(),
            Byte.class, new StringToByteConverter(),
            Short.class, new StringToShortConverter(),
            Integer.class, new StringToIntegerConverter(),
            Long.class, new StringToLongConverter(),
            Boolean.class, new StringToBooleanConverter(),
            Float.class, new StringToFloatConverter(),
            Double.class, new StringToDoubleConverter()
    );

    public static <T> Converter<String, T> obtainStringConverter(ObjectMapper objectMapper, Class<T> msgType) {
        var converter = STRING_CONVERTER_MAP.get(msgType);
        if (converter == null) {
            converter = new StringToObjectConverter(objectMapper, msgType);
        }
        // noinspection unchecked
        return (Converter<String, T>) converter;
    }

    public static class StringToStringConverter implements Converter<String, String> {
        @Override
        public String convert(@Nonnull String msg) {
            return msg;
        }
    }

    public static class StringToByteConverter implements Converter<String, Byte> {
        @Override
        public Byte convert(@Nonnull String msg) {
            return Byte.valueOf(msg);
        }
    }

    public static class StringToShortConverter implements Converter<String, Short> {
        @Override
        public Short convert(@Nonnull String msg) {
            return Short.valueOf(msg);
        }
    }

    public static class StringToIntegerConverter implements Converter<String, Integer> {
        @Override
        public Integer convert(@Nonnull String msg) {
            return Integer.valueOf(msg);
        }
    }

    public static class StringToLongConverter implements Converter<String, Long> {
        @Override
        public Long convert(@Nonnull String msg) {
            return Long.valueOf(msg);
        }
    }

    public static class StringToBooleanConverter implements Converter<String, Boolean> {
        @Override
        public Boolean convert(@Nonnull String msg) {
            return Boolean.valueOf(msg);
        }
    }

    public static class StringToFloatConverter implements Converter<String, Float> {
        @Override
        public Float convert(@Nonnull String msg) {
            return Float.valueOf(msg);
        }
    }

    public static class StringToDoubleConverter implements Converter<String, Double> {
        @Override
        public Double convert(@Nonnull String msg) {
            return Double.valueOf(msg);
        }
    }

    public static class StringToObjectConverter implements Converter<String, Object> {
        private final ObjectMapper objectMapper;
        private final Class<?> msgType;

        public StringToObjectConverter(ObjectMapper objectMapper, Class<?> msgType) {
            this.objectMapper = objectMapper;
            this.msgType = msgType;
        }

        @Override
        @SneakyThrows
        public Object convert(@Nonnull String msg) {
            return objectMapper.readValue(msg, msgType);
        }
    }
}
