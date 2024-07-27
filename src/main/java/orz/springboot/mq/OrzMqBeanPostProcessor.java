package orz.springboot.mq;

import jakarta.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.Ordered;
import org.springframework.stereotype.Component;

import static orz.springboot.base.description.OrzDescriptionUtils.desc;

@Slf4j
@Component
public class OrzMqBeanPostProcessor implements BeanPostProcessor, ApplicationContextAware, Ordered {
    private final OrzMqManager mqManager;

    private OrzMqBeanInitContext beanInitContext = new OrzMqBeanInitContext(null);

    @Lazy
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
    public Object postProcessBeforeInitialization(@Nonnull Object bean, @Nonnull String beanName) throws BeansException {
        if (bean instanceof OrzMqPub<?, ?> pub) {
            pub.init(beanInitContext);
            mqManager.registerPub(pub);
            if (log.isDebugEnabled()) {
                log.debug(desc("register pub", "pub", pub.getClass()));
            }
        } else if (bean instanceof OrzMqSub<?, ?> sub) {
            sub.init(beanInitContext);
            mqManager.registerSub(sub);
            if (log.isDebugEnabled()) {
                log.debug(desc("register sub", "sub", sub.getClass()));
            }
        }
        return BeanPostProcessor.super.postProcessBeforeInitialization(bean, beanName);
    }

    @Override
    public Object postProcessAfterInitialization(@Nonnull Object bean, @Nonnull String beanName) throws BeansException {
        return BeanPostProcessor.super.postProcessAfterInitialization(bean, beanName);
    }
}
