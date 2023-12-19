package cn.tmkit.mq.rocketmq5.boot.consumer.listener;

import cn.tmkit.core.convert.ConvertUtil;
import cn.tmkit.core.lang.Asserts;
import cn.tmkit.core.lang.Strings;
import cn.tmkit.mq.rocketmq5.boot.consumer.annotation.RocketMQMessageConsumer;
import cn.tmkit.mq.rocketmq5.boot.serializer.RocketMQMessageSerializer;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 处理带有注解{@linkplain RocketMQMessageConsumer}的bean，
 *
 * @author ming.tang
 * @version 0.0.1
 * @date 2023-12-18
 */
@Slf4j
@SuppressWarnings("unchecked")
public class RocketMQMessageListenerBeanPostProcessor implements ApplicationContextAware, BeanPostProcessor {

    private final AtomicLong counter = new AtomicLong(0);

    private GenericApplicationContext applicationContext;

    @Override
    public Object postProcessBeforeInitialization(@NotNull Object bean, @NotNull String beanName) throws BeansException {
        return bean;
    }

    @Override
    public Object postProcessAfterInitialization(@NotNull Object bean, @NotNull String beanName) throws BeansException {
        Class<?> targetClass = AopUtils.getTargetClass(bean);
        RocketMQMessageConsumer rocketMQMessageConsumer = targetClass.getAnnotation(RocketMQMessageConsumer.class);
        if (rocketMQMessageConsumer != null) {
            registerContainer(beanName, bean, rocketMQMessageConsumer);
        }
        return bean;
    }

    @Override
    public void setApplicationContext(@NotNull ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = (GenericApplicationContext) applicationContext;
    }

    private void registerContainer(String beanName, Object bean, RocketMQMessageConsumer annotation) {
        validate(annotation);
        String containerBeanName = Strings.format("{}_{}", DefaultRocketMQListenerContainer.class.getName(), counter.getAndIncrement());
        applicationContext.registerBean(containerBeanName, DefaultRocketMQListenerContainer.class,
                () -> createDefaultRocketMQListenerContainer(containerBeanName, bean, annotation));
        DefaultRocketMQListenerContainer container = applicationContext.getBean(containerBeanName, DefaultRocketMQListenerContainer.class);
        if (!container.isRunning()) {
            container.start();
        }
        log.debug("Register the listener to container, listenerBeanName:{}, containerBeanName:{}", beanName, containerBeanName);
    }

    private void validate(RocketMQMessageConsumer annotation) {
        Asserts.notEmpty(annotation.accessKey(), "[accessKey] must not be null");
        Asserts.notEmpty(annotation.secretKey(), "[secretKey] must not be null");
        Asserts.notEmpty(annotation.topic(), "[topic] must not be null");
        Asserts.notEmpty(annotation.endpoints(), "[endpoints] must not be null");
    }

    private DefaultRocketMQListenerContainer createDefaultRocketMQListenerContainer(String name, Object bean, RocketMQMessageConsumer annotation) {
        ConfigurableEnvironment environment = applicationContext.getEnvironment();
        RocketMQMessageSerializer<Object> rocketMQMessageSerializer = applicationContext.getBean(RocketMQMessageSerializer.class);
        DefaultRocketMQListenerContainer container = new DefaultRocketMQListenerContainer();
        container.setRocketMQMessageSerializer(rocketMQMessageSerializer);
        container.setName(name);
        container.setRocketMQMessageListener((RocketMQMessageListener<Object>) bean);
        container.setRocketMQMessageConsumer(annotation);
        container.setEndpoints(environment.resolvePlaceholders(annotation.endpoints()));
        container.setConsumerGroup(environment.resolvePlaceholders(annotation.group()));
        container.setConsumptionThreadCount(ConvertUtil.toInt(environment.resolvePlaceholders(annotation.consumptionThreadCount())));
        container.setTopic(environment.resolvePlaceholders(annotation.topic()));
        container.setFilterType(annotation.filterExpressionType());
        container.setFilterExpression(environment.resolvePlaceholders(annotation.filterExpression()));
        container.setEnableSsl(ConvertUtil.toBool(environment.resolvePlaceholders(annotation.enableSsl())));
        container.setRequestTimeout(Duration.ofSeconds(annotation.requestTimeout()));
        container.setAccessKey(environment.resolvePlaceholders(annotation.accessKey()));
        container.setSecretKey(environment.resolvePlaceholders(annotation.secretKey()));
        container.setMaxCachedMessageCount(annotation.maxCachedMessageCount());
        container.setMaxCacheMessageSizeInBytes(annotation.maxCacheMessageSizeInBytes());
        return container;
    }

}
