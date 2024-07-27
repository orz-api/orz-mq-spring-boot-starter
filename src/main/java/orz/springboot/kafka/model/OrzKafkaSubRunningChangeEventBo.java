package orz.springboot.kafka.model;

import lombok.Getter;
import org.springframework.context.ApplicationEvent;
import orz.springboot.kafka.OrzKafkaBaseSub;

@Getter
public class OrzKafkaSubRunningChangeEventBo extends ApplicationEvent {
    private final OrzKafkaBaseSub<?> sub;
    private final boolean running;

    public OrzKafkaSubRunningChangeEventBo(OrzKafkaBaseSub<?> sub, boolean running) {
        super(sub);
        this.sub = sub;
        this.running = running;
    }
}
