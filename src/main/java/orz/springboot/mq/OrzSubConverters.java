package orz.springboot.mq;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.springframework.core.convert.converter.Converter;

import javax.annotation.Nonnull;
import java.util.Map;

public class OrzSubConverters {
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

    public static <T> Converter<String, T> obtainStringConverter(ObjectMapper objectMapper, Class<T> messageType) {
        var converter = STRING_CONVERTER_MAP.get(messageType);
        if (converter == null) {
            converter = new StringToObjectConverter(objectMapper, messageType);
        }
        // noinspection unchecked
        return (Converter<String, T>) converter;
    }

    public static class StringToStringConverter implements Converter<String, String> {
        @Override
        public String convert(@Nonnull String message) {
            return message;
        }
    }

    public static class StringToByteConverter implements Converter<String, Byte> {
        @Override
        public Byte convert(@Nonnull String message) {
            return Byte.valueOf(message);
        }
    }

    public static class StringToShortConverter implements Converter<String, Short> {
        @Override
        public Short convert(@Nonnull String message) {
            return Short.valueOf(message);
        }
    }

    public static class StringToIntegerConverter implements Converter<String, Integer> {
        @Override
        public Integer convert(@Nonnull String message) {
            return Integer.valueOf(message);
        }
    }

    public static class StringToLongConverter implements Converter<String, Long> {
        @Override
        public Long convert(@Nonnull String message) {
            return Long.valueOf(message);
        }
    }

    public static class StringToBooleanConverter implements Converter<String, Boolean> {
        @Override
        public Boolean convert(@Nonnull String message) {
            return Boolean.valueOf(message);
        }
    }

    public static class StringToFloatConverter implements Converter<String, Float> {
        @Override
        public Float convert(@Nonnull String message) {
            return Float.valueOf(message);
        }
    }

    public static class StringToDoubleConverter implements Converter<String, Double> {
        @Override
        public Double convert(@Nonnull String message) {
            return Double.valueOf(message);
        }
    }

    public static class StringToObjectConverter implements Converter<String, Object> {
        private final ObjectMapper objectMapper;
        private final Class<?> messageType;

        public StringToObjectConverter(ObjectMapper objectMapper, Class<?> messageType) {
            this.objectMapper = objectMapper;
            this.messageType = messageType;
        }

        @Override
        @SneakyThrows
        public Object convert(@Nonnull String message) {
            return objectMapper.readValue(message, messageType);
        }
    }
}
