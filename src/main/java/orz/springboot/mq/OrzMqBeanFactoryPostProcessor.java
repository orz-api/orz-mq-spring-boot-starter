package orz.springboot.mq;

import jakarta.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.core.ResolvableType;
import org.springframework.stereotype.Component;

import static orz.springboot.base.OrzBaseUtils.assertion;
import static orz.springboot.base.description.OrzDescriptionUtils.desc;

@Slf4j
@Component
public class OrzMqBeanFactoryPostProcessor implements BeanFactoryPostProcessor {
    @Override
    public void postProcessBeanFactory(@Nonnull ConfigurableListableBeanFactory beanFactory) throws BeansException {
        assertion(beanFactory instanceof BeanDefinitionRegistry, "beanFactory instanceof BeanDefinitionRegistry");
        var registry = (BeanDefinitionRegistry) beanFactory;

        // 将默认生成的不带泛型的 bean 移除掉
        // 可以避免注入错误的泛型时不报错的情况
        removeDefaultPublisher(registry);

        for (var name : beanFactory.getBeanDefinitionNames()) {
            var definition = beanFactory.getBeanDefinition(name);
            if (StringUtils.isNotBlank(definition.getBeanClassName())) {
                var cls = (Class<?>) null;
                try {
                    cls = Class.forName(definition.getBeanClassName());
                } catch (ClassNotFoundException e) {
                    continue;
                }
                if (OrzMqPub.class.isAssignableFrom(cls)) {
                    registerPublisher(registry, cls);
                }
            }
        }
    }

    private void removeDefaultPublisher(BeanDefinitionRegistry registry) {
        registry.removeBeanDefinition(OrzMqPublisher.class.getName());
        if (log.isDebugEnabled()) {
            log.debug(desc("remove default publisher bean"));
        }
    }

    private void registerPublisher(BeanDefinitionRegistry registry, Class<?> cls) {
        var dataType = OrzMqUtils.getPubDataType(cls);
        if (dataType == null) {
            return;
        }
        var definition = new RootBeanDefinition(OrzMqPublisher.class);
        definition.setTargetType(ResolvableType.forClassWithGenerics(OrzMqPublisher.class, dataType));
        registry.registerBeanDefinition("OrzMqPublisher<" + dataType + ">", definition);
        if (log.isDebugEnabled()) {
            log.debug(desc("register publisher bean", "publisher", "OrzMqPublisher<" + dataType + ">"));
        }
    }
}
