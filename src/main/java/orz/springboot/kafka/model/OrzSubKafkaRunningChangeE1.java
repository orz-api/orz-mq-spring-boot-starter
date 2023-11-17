package orz.springboot.kafka.model;

import lombok.Getter;
import org.springframework.context.ApplicationEvent;
import orz.springboot.kafka.OrzSubKafka;

@Getter
public class OrzSubKafkaRunningChangeE1 extends ApplicationEvent {
    private final OrzSubKafka<?> sub;
    private final boolean running;

    public OrzSubKafkaRunningChangeE1(OrzSubKafka<?> sub, boolean running) {
        super(sub);
        this.sub = sub;
        this.running = running;
    }
}
