package orz.springboot.mq.api.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TestStringEventBo {
    private String key;
    private String message;
}
