package cn.tmkit.mq.rocketmq5.boot.autoconfigure;

import cn.tmkit.core.convert.ConvertUtil;
import cn.tmkit.core.lang.Collections;
import cn.tmkit.core.lang.Maps;
import cn.tmkit.core.lang.Strings;
import cn.tmkit.mq.rocketmq5.boot.core.RocketMQTemplate;
import cn.tmkit.mq.rocketmq5.boot.serializer.RocketMQMessageSerializer;
import cn.tmkit.mq.rocketmq5.boot.util.RocketMQUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.ProducerBuilder;
import org.apache.rocketmq.client.java.impl.ClientImpl;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.condition.*;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.*;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.Environment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.Assert;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 自动装配RocketMQ
 *
 * @author ming.tang
 * @version 0.0.1
 * @date 2023-12-18
 */
@Slf4j
@Configuration
@RequiredArgsConstructor
@ConditionalOnClass({ClientImpl.class})
@EnableConfigurationProperties(RocketMQProperties.class)
@Import({RocketMQListenerConfiguration.class})
@ConditionalOnProperty(prefix = RocketMQProperties.PREFIX, value = "endpoints")
public class RocketMQAutoConfiguration implements InitializingBean, ApplicationContextAware, EnvironmentPostProcessor {

    public static final String ROCKETMQ_TEMPLATE_DEFAULT_GLOBAL_NAME = "rocketMQTemplate";
    public static final String PRODUCER_BEAN_NAME = "defaultRocketMQProducer";
    public static final String CONSUMER_BEAN_NAME = "defaultLitePullConsumer";

    private ApplicationContext applicationContext;

    /**
     * {@inheritDoc}
     */
    @Override
    public void setApplicationContext(@NotNull ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    /**
     * Post-process the given {@code environment}.
     *
     * @param environment the environment to post-process
     * @param application the application to which the environment belongs
     */
    @Override
    public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
        log.info("postProcessEnvironment execute ...");

        String defaultTopicKey = RocketMQProperties.PREFIX + ".default-topic";
        String value = environment.getProperty(defaultTopicKey);
        if (Strings.isEmpty(value)) {
            String bindTopicsKey = RocketMQProperties.PREFIX + ".bind-topics";
            List<?> bindTopics = environment.getProperty(bindTopicsKey, List.class);
            if (Collections.isNotEmpty(bindTopics)) {
                Map<String, Object> sourceMap = Maps.hashMap(16);
                sourceMap.put(defaultTopicKey, ConvertUtil.toStr(bindTopics.get(0)));
                MutablePropertySources propertySources = environment.getPropertySources();
                MapPropertySource propertySource = new MapPropertySource("spring-boot-rocketmq-program", sourceMap);
                propertySources.addLast(propertySource);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void afterPropertiesSet() {
        RocketMQProperties rocketMQProperties = applicationContext.getBean(RocketMQProperties.class);
        String endpoints = rocketMQProperties.getEndpoints();
        if (endpoints == null) {
            log.warn("The necessary spring property '" + RocketMQProperties.PREFIX + ".endpoints' is not defined, all rocketmq beans creation are skipped!");
            return;
        }
        RocketMQProperties.Producer producerConfig = rocketMQProperties.getProducer();
        if (producerConfig == null) {
            log.warn("The necessary spring property '" + RocketMQProperties.PREFIX + ".producer' is not defined, all rocketmq beans creation are skipped!");
        } else {
            List<String> bindTopics = producerConfig.getBindTopics();
            if (Collections.isNotEmpty(bindTopics) && Strings.isEmpty(producerConfig.getDefaultTopic())) {
                producerConfig.setDefaultTopic(bindTopics.get(0));
            }
            if (Objects.isNull(producerConfig.getAccessKey())) {
                producerConfig.setAccessKey(rocketMQProperties.getAccessKey());
            }
            if (Objects.isNull(producerConfig.getSecretKey())) {
                producerConfig.setSecretKey(rocketMQProperties.getSecretKey());
            }
            if (Objects.isNull(producerConfig.getRequestTimeout())) {
                producerConfig.setRequestTimeout(rocketMQProperties.getRequestTimeout());
            }
        }
    }

    /**
     * 生产者
     *
     * @param rocketMQProperties 配置
     * @return {@linkplain Producer}
     * @throws ClientException 客户端异常
     */
    @Bean(PRODUCER_BEAN_NAME)
    @ConditionalOnMissingBean(Producer.class)
    @ConditionalOnExpression
    @ConditionalOnRocketMQProducerProperties
    @ConditionalOnProperty(prefix = RocketMQProperties.PREFIX, value = {"endpoints"})
    public Producer defaultRocketMQProducer(RocketMQProperties rocketMQProperties) throws ClientException {
        RocketMQProperties.Producer producerConfig = rocketMQProperties.getProducer();
        String endpoints = rocketMQProperties.getEndpoints();
        String defaultTopic = producerConfig.getDefaultTopic();
        Assert.hasText(endpoints, RocketMQProperties.PREFIX + "[.endpoints] must not be null");
        Assert.hasText(defaultTopic, RocketMQProperties.PREFIX + "[.producer.default-topic] must not be null");

        ClientServiceProvider clientServiceProvider = RocketMQUtils.getClientServiceProvider();
        ProducerBuilder producerBuilder = clientServiceProvider.newProducerBuilder();
        // 绑定主题
        if (Collections.isNotEmpty(producerConfig.getBindTopics())) {
            producerConfig.getBindTopics().forEach(producerBuilder::setTopics);
        }
        Producer producer = producerBuilder.setMaxAttempts(producerConfig.getMaxAttempts())
                // 客户端配置
                .setClientConfiguration(RocketMQUtils.createClientConfiguration(rocketMQProperties.getEndpoints(),
                        rocketMQProperties.isEnableSsl(), producerConfig.getAccessKey(), producerConfig.getSecretKey(),
                        Duration.ofMillis(producerConfig.getRequestTimeout())))
                .build();
        log.info("{} started successful on endpoints {}", PRODUCER_BEAN_NAME, endpoints);
        return producer;
    }

    /**
     * 消息模板
     *
     * @param rocketMQMessageSerializer 序列化器
     * @return {@linkplain RocketMQTemplate}
     */
    @Bean(destroyMethod = "destroy")
    @Conditional(ProducerOrConsumerPropertyCondition.class)
    @ConditionalOnMissingBean(name = ROCKETMQ_TEMPLATE_DEFAULT_GLOBAL_NAME)
    public RocketMQTemplate rocketMQTemplate(RocketMQMessageSerializer<Object> rocketMQMessageSerializer,
                                             RocketMQProperties rocketMQProperties,
                                             ThreadPoolTaskExecutor asyncSendThreadPoolTaskExecutor) {
        RocketMQTemplate rocketMQTemplate = new RocketMQTemplate();
        if (applicationContext.containsBean(PRODUCER_BEAN_NAME)) {
            rocketMQTemplate.setProducer(applicationContext.getBean(PRODUCER_BEAN_NAME, Producer.class));
        }
        rocketMQTemplate.setRocketMQMessageSerializer(rocketMQMessageSerializer);
        rocketMQTemplate.setDefaultTopic(rocketMQProperties.getProducer().getDefaultTopic());
        rocketMQTemplate.setAsyncSendThreadPoolTaskExecutor(asyncSendThreadPoolTaskExecutor);
        return rocketMQTemplate;
    }

    @Bean
    @ConditionalOnMissingBean
    public RocketMQMessageSerializer<Object> rocketMQMessageSerializer() {
        return RocketMQMessageSerializer.jackson();
    }

    @Bean
    @ConditionalOnMissingBean
    public ThreadPoolTaskExecutor asyncSendThreadPoolTaskExecutor() {
        return new ThreadPoolTaskExecutor();
    }

    static class ProducerOrConsumerPropertyCondition extends AnyNestedCondition {

        public ProducerOrConsumerPropertyCondition() {
            super(ConfigurationPhase.REGISTER_BEAN);
        }

        @ConditionalOnBean(Producer.class)
        static class DefaultMQProducerExistsCondition {
        }

        @ConditionalOnBean(PushConsumer.class)
        static class DefaultLitePullConsumerExistsCondition {
        }

    }

    @Target({ElementType.TYPE, ElementType.METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    @Conditional(OnRocketMQProducerPropertiesCondition.class)
    @interface ConditionalOnRocketMQProducerProperties {

    }

    static class OnRocketMQProducerPropertiesCondition implements Condition {

        @Override
        public boolean matches(@NotNull ConditionContext context, @NotNull AnnotatedTypeMetadata metadata) {
            Environment env = context.getEnvironment();
            String prefix = RocketMQProperties.PREFIX + ".producer";
            return env.containsProperty(prefix + ".default-topic") || env.containsProperty(prefix + ".bind-topics[0]");
        }

    }

}
