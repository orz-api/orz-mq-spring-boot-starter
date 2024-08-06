package orz.springboot.mq.api.model;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.type.Date;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TestProtobufSchemaEventBo {
    private byte[] bytesField;

    private Integer intField;
    private List<Integer> intListField;

    private Long longField;
    private List<Long> longListField;

    private Float floatField;
    private List<Float> floatListField;

    private Double doubleField;
    private List<Double> doubleListField;

    private String strField;
    private List<String> strListField;

    private InnerObjectBo innerObjectField;
    private List<InnerObjectBo> innerObjectListField;

    private LocalDateTime timeField;
    private LocalDate dateField;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class InnerObjectBo {
        private String str;

        public static TestProtobufInnerObjectV1To toInnerObjectV1To(InnerObjectBo bo) {
            return bo == null ? null : TestProtobufInnerObjectV1To.newBuilder().setStr(bo.getStr()).build();
        }

        public static List<TestProtobufInnerObjectV1To> toInnerObjectV1ToList(List<InnerObjectBo> list) {
            if (list == null) {
                return null;
            }
            return list.stream().map(InnerObjectBo::toInnerObjectV1To).toList();
        }
    }

    public TestProtobufV1To toTestProtobufV1To() {
        var builder = TestProtobufV1To.newBuilder();
        if (bytesField != null) {
            builder.setBytesField(ByteString.copyFrom(bytesField));
        }
        if (intField != null) {
            builder.setIntField(intField);
        }
        if (intListField != null) {
            builder.addAllIntListField(intListField);
        }
        if (longField != null) {
            builder.setLongField(longField);
        }
        if (longListField != null) {
            builder.addAllLongListField(longListField);
        }
        if (floatField != null) {
            builder.setFloatField(floatField);
        }
        if (floatListField != null) {
            builder.addAllFloatListField(floatListField);
        }
        if (doubleField != null) {
            builder.setDoubleField(doubleField);
        }
        if (doubleListField != null) {
            builder.addAllDoubleListField(doubleListField);
        }
        if (strField != null) {
            builder.setStrField(strField);
        }
        if (strListField != null) {
            builder.addAllStrListField(strListField);
        }
        if (innerObjectField != null) {
            builder.setInnerObjectField(InnerObjectBo.toInnerObjectV1To(innerObjectField));
        }
        if (innerObjectListField != null) {
            builder.addAllInnerObjectListField(InnerObjectBo.toInnerObjectV1ToList(innerObjectListField));
        }
        if (timeField != null) {
            builder.setTimeField(Timestamp.newBuilder()
                    .setSeconds(timeField.toEpochSecond(ZoneOffset.ofHours(8)))
                    .setNanos(timeField.getNano())
            );
        }
        if (dateField != null) {
            builder.setDateField(Date.newBuilder()
                    .setYear(dateField.getYear())
                    .setMonth(dateField.getMonthValue())
                    .setDay(dateField.getDayOfYear()));
        }
        return builder.build();
    }
}
