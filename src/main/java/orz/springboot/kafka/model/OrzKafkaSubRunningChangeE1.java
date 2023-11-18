package orz.springboot.kafka.model;

import lombok.Getter;
import org.springframework.context.ApplicationEvent;
import orz.springboot.kafka.OrzKafkaSub;

@Getter
public class OrzKafkaSubRunningChangeE1 extends ApplicationEvent {
    private final OrzKafkaSub<?> sub;
    private final boolean running;

    public OrzKafkaSubRunningChangeE1(OrzKafkaSub<?> sub, boolean running) {
        super(sub);
        this.sub = sub;
        this.running = running;
    }
}
