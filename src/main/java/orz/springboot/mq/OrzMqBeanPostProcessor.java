package orz.springboot.mq;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.Ordered;
import org.springframework.core.ResolvableType;

import javax.annotation.Nonnull;

@Slf4j
public class OrzMqBeanPostProcessor implements BeanPostProcessor, ApplicationContextAware, Ordered {
    private final OrzMq mq;

    private OrzMqBeanInitContext beanInitContext = new OrzMqBeanInitContext(null);

    public OrzMqBeanPostProcessor(OrzMq mq) {
        this.mq = mq;
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
        if (bean instanceof OrzPubBase<?> pub) {
            pub.init(beanInitContext);
            mq.registerPub(pub);
            var definition = new RootBeanDefinition(OrzPub.class, () -> new OrzPub<>(mq));
            definition.setTargetType(ResolvableType.forClassWithGenerics(OrzPub.class, pub.getEventType()));
            beanInitContext.getBeanDefinitionRegistry().registerBeanDefinition("OrzPub<" + pub.getEventType() + ">", definition);
        } else if (bean instanceof OrzSubBase<?, ?> sub) {
            sub.init(beanInitContext);
            mq.registerSub(sub);
        } else if (bean instanceof OrzPub<?> pub) {
            // 将默认生成的不带泛型的 bean 移除掉
            // 可以避免注入错误的泛型时不报错的情况
            if (beanName.equals(pub.getClass().getName())) {
                beanInitContext.getBeanDefinitionRegistry().removeBeanDefinition(beanName);
            }
        }
        return BeanPostProcessor.super.postProcessAfterInitialization(bean, beanName);
    }
}
