package orz.springboot.mq.api.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TestJsonSchemaEventBo {
    private byte byteField;
    private byte[] byteArrayField;

    private short shortField;
    private short[] shortArrayField;

    private int intField;
    private int[] intArrayField;

    private long longField;
    private long[] longArrayField;

    private float floatField;
    private float[] floatArrayField;

    private double doubleField;
    private double[] doubleArrayField;

    private char charField;
    private char[] charArrayField;

    private Byte byteObjField;
    private Byte[] byteObjArrayField;
    private List<Byte> byteObjListField;

    private Short shortObjField;
    private Short[] shortObjArrayField;
    private List<Short> shortObjListField;

    private Integer intObjField;
    private Integer[] intObjArrayField;
    private List<Integer> intObjListField;

    private Long longObjField;
    private Long[] longObjArrayField;
    private List<Long> longObjListField;

    private Float floatObjField;
    private Float[] floatObjArrayField;
    private List<Float> floatObjListField;

    private Double doubleObjField;
    private Double[] doubleObjArrayField;
    private List<Double> doubleObjListField;

    private Character charObjField;
    private Character[] charObjArrayField;
    private List<Character> charObjListField;

    private String strField;
    private String[] strArrayField;
    private List<String> strListField;

    private InnerObjectBo innerObjectField;
    private InnerObjectBo[] innerObjectArrayField;
    private List<InnerObjectBo> innerObjectListField;

    private LocalDateTime timeField;
    private LocalDate dateField;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class InnerObjectBo {
        private String str;

        public static TestJsonV1To.InnerObjectV1To toInnerObjectV1To(InnerObjectBo bo) {
            return bo == null ? null : new TestJsonV1To.InnerObjectV1To(bo.str);
        }

        public static TestJsonV1To.InnerObjectV1To[] toInnerObjectV1ToArray(InnerObjectBo[] array) {
            if (array == null) {
                return null;
            }
            TestJsonV1To.InnerObjectV1To[] result = new TestJsonV1To.InnerObjectV1To[array.length];
            for (int i = 0; i < array.length; i++) {
                result[i] = toInnerObjectV1To(array[i]);
            }
            return result;
        }

        public static List<TestJsonV1To.InnerObjectV1To> toInnerObjectV1ToList(List<InnerObjectBo> list) {
            if (list == null) {
                return null;
            }
            return list.stream().map(InnerObjectBo::toInnerObjectV1To).toList();
        }
    }

    public TestJsonV1To toTestJsonV1To() {
        return new TestJsonV1To(
                byteField,
                byteArrayField,
                shortField,
                shortArrayField,
                intField,
                intArrayField,
                longField,
                longArrayField,
                floatField,
                floatArrayField,
                doubleField,
                doubleArrayField,
                charField,
                charArrayField,
                byteObjField,
                byteObjArrayField,
                byteObjListField,
                shortObjField,
                shortObjArrayField,
                shortObjListField,
                intObjField,
                intObjArrayField,
                intObjListField,
                longObjField,
                longObjArrayField,
                longObjListField,
                floatObjField,
                floatObjArrayField,
                floatObjListField,
                doubleObjField,
                doubleObjArrayField,
                doubleObjListField,
                charObjField,
                charObjArrayField,
                charObjListField,
                strField,
                strArrayField,
                strListField,
                InnerObjectBo.toInnerObjectV1To(innerObjectField),
                InnerObjectBo.toInnerObjectV1ToArray(innerObjectArrayField),
                InnerObjectBo.toInnerObjectV1ToList(innerObjectListField),
                timeField,
                dateField
        );
    }
}
