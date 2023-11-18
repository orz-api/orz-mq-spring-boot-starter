package orz.springboot.mq;

import org.springframework.beans.factory.config.BeanExpressionContext;
import org.springframework.beans.factory.config.BeanExpressionResolver;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.expression.StandardBeanExpressionResolver;
import orz.springboot.base.OrzBaseUtils;

import static orz.springboot.base.OrzBaseUtils.message;

public class OrzMqBeanInitContext {
    private final ConfigurableApplicationContext context;
    private final BeanExpressionResolver expressionResolver;
    private final BeanExpressionContext expressionContext;

    public OrzMqBeanInitContext(ConfigurableApplicationContext context) {
        if (context != null) {
            this.context = context;
            this.expressionResolver = context.getBeanFactory().getBeanExpressionResolver();
            this.expressionContext = new BeanExpressionContext(context.getBeanFactory(), null);
        } else {
            this.context = null;
            this.expressionResolver = new StandardBeanExpressionResolver();
            this.expressionContext = null;
        }
    }

    public String resolveExpressionAsString(String value) {
        var result = resolveExpression(value);
        if (result == null) {
            return null;
        } else if (result instanceof String) {
            return (String) result;
        } else if (result instanceof CharSequence) {
            return result.toString();
        } else if (result instanceof Number) {
            return result.toString();
        } else if (result instanceof Boolean) {
            return result.toString();
        }
        throw new RuntimeException(message(
                "resolve expression as string failed",
                "value", value, "result", result, "resultClass", result.getClass()
        ));
    }

    public Object resolveExpression(String value) {
        return expressionResolver.evaluate(resolve(value), expressionContext);
    }

    public String resolve(String value) {
        if (context != null) {
            return context.getBeanFactory().resolveEmbeddedValue(value);
        }
        return value;
    }

    public ConfigurableApplicationContext getApplicationContext() {
        if (context == null) {
            return OrzBaseUtils.getAppContext();
        }
        return context;
    }

    public BeanDefinitionRegistry getBeanDefinitionRegistry() {
        return (BeanDefinitionRegistry) getApplicationContext().getBeanFactory();
    }
}
