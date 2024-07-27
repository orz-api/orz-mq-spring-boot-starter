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
public class TestJsonV1To {
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

    private InnerObjectV1To innerObjectField;
    private InnerObjectV1To[] innerObjectArrayField;
    private List<InnerObjectV1To> innerObjectListField;

    private LocalDateTime timeField;
    private LocalDate dateField;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class InnerObjectV1To {
        private String str;
    }
}
