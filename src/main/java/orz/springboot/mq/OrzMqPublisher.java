package orz.springboot.mq;

import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

// 为了避免 IDEA 提示 "Could not autowire."
// 需要将其注册到 org.springframework.boot.autoconfigure.AutoConfiguration.imports
// 同时需要在运行时移除掉 bean 的定义，不然使用者注入错误的泛型时也不会报错
// 移除逻辑见 OrzMqBeanFactoryPostProcessor
@Component
public class OrzMqPublisher<D> {
    private final OrzMqManager mqManager;

    public OrzMqPublisher(OrzMqManager mqManager) {
        this.mqManager = mqManager;
    }

    public void publish(D data) throws Exception {
        mqManager.publish(data);
    }

    public CompletableFuture<Void> publishAsync(D data) {
        return mqManager.publishAsync(data);
    }
}
