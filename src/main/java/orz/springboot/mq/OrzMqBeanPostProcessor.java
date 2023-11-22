package orz.springboot.mq;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.Ordered;
import org.springframework.core.ResolvableType;
import org.springframework.stereotype.Component;

import javax.annotation.Nonnull;

@Component
public class OrzMqBeanPostProcessor implements BeanPostProcessor, ApplicationContextAware, Ordered {
    private final OrzMqManager mqManager;

    private OrzMqBeanInitContext beanInitContext = new OrzMqBeanInitContext(null);

    public OrzMqBeanPostProcessor(OrzMqManager mqManager) {
        this.mqManager = mqManager;
    }

    @Override
    public void setApplicationContext(@Nonnull ApplicationContext ctx) throws BeansException {
        if (ctx instanceof ConfigurableApplicationContext c) {
            beanInitContext = new OrzMqBeanInitContext(c);
            beanInitContext.getApplicationContext().getBeanFactory();
        }
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }

    @Override
    public Object postProcessAfterInitialization(@Nonnull Object bean, @Nonnull String beanName) throws BeansException {
        if (bean instanceof OrzMqPub<?> pub) {
            pub.init(beanInitContext);
            mqManager.registerPub(pub);
            var definition = new RootBeanDefinition(OrzMqPublisher.class, () -> new OrzMqPublisher<>(mqManager));
            definition.setTargetType(ResolvableType.forClassWithGenerics(OrzMqPublisher.class, pub.getEventType()));
            beanInitContext.getBeanDefinitionRegistry().registerBeanDefinition("OrzMqPublisher<" + pub.getEventType() + ">", definition);
        } else if (bean instanceof OrzMqSub<?, ?> sub) {
            sub.init(beanInitContext);
            mqManager.registerSub(sub);
        } else if (bean instanceof OrzMqPublisher<?> pub) {
            // 将默认生成的不带泛型的 bean 移除掉
            // 可以避免注入错误的泛型时不报错的情况
            if (beanName.equals(pub.getClass().getName())) {
                beanInitContext.getBeanDefinitionRegistry().removeBeanDefinition(beanName);
            }
        }
        return BeanPostProcessor.super.postProcessAfterInitialization(bean, beanName);
    }
}
