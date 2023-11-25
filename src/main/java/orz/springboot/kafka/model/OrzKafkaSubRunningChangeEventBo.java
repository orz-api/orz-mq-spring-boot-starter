package orz.springboot.kafka.model;

import lombok.Getter;
import org.springframework.context.ApplicationEvent;
import orz.springboot.kafka.OrzKafkaSub;

@Getter
public class OrzKafkaSubRunningChangeEventBo extends ApplicationEvent {
    private final OrzKafkaSub<?> sub;
    private final boolean running;

    public OrzKafkaSubRunningChangeEventBo(OrzKafkaSub<?> sub, boolean running) {
        super(sub);
        this.sub = sub;
        this.running = running;
    }
}
