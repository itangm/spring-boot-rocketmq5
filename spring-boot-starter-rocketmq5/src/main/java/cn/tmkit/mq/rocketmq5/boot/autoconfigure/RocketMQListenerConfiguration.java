package cn.tmkit.mq.rocketmq5.boot.autoconfigure;

import cn.tmkit.mq.rocketmq5.boot.consumer.listener.RocketMQMessageListenerBeanPostProcessor;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.type.AnnotationMetadata;

/**
 * @author ming.tang
 * @version 0.0.1
 * @date 2023-12-18
 */
@Configuration
@AutoConfigureAfter(RocketMQAutoConfiguration.class)
public class RocketMQListenerConfiguration implements ImportBeanDefinitionRegistrar {

    @Override
    public void registerBeanDefinitions(@NotNull AnnotationMetadata metadata, BeanDefinitionRegistry registry) {
        if (!registry.containsBeanDefinition(RocketMQMessageListenerBeanPostProcessor.class.getName())) {
            registry.registerBeanDefinition(RocketMQMessageListenerBeanPostProcessor.class.getName(),
                    new RootBeanDefinition(RocketMQMessageListenerBeanPostProcessor.class));
        }
    }

}
