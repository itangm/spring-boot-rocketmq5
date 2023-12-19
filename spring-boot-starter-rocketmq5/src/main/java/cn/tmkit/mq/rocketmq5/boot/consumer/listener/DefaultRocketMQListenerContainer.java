package cn.tmkit.mq.rocketmq5.boot.consumer.listener;

import cn.tmkit.core.io.NioUtil;
import cn.tmkit.core.lang.Asserts;
import cn.tmkit.core.lang.Maps;
import cn.tmkit.mq.rocketmq5.boot.consumer.annotation.RocketMQMessageConsumer;
import cn.tmkit.mq.rocketmq5.boot.serializer.RocketMQMessageSerializer;
import cn.tmkit.mq.rocketmq5.boot.util.RocketMQUtils;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.FilterExpressionType;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
import org.apache.rocketmq.client.apis.consumer.PushConsumerBuilder;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.SmartLifecycle;

import java.io.IOException;
import java.time.Duration;
import java.util.Objects;

/**
 * {@linkplain RocketMQListenerContainer}的默认实现
 *
 * @author ming.tang
 * @version 0.0.1
 * @date 2023-12-18
 */
@Slf4j
@Getter
@Setter
@NoArgsConstructor
public class DefaultRocketMQListenerContainer implements RocketMQListenerContainer, SmartLifecycle, ApplicationContextAware {

    private ApplicationContext applicationContext;

    private RocketMQMessageSerializer<Object> rocketMQMessageSerializer;

    /**
     * The name of the DefaultRocketMQListenerContainer instance
     */
    private String name;

    /**
     * 容器是否正在运行
     */
    private boolean running;

    /**
     * 消息消费者
     */
    @Setter(AccessLevel.PRIVATE)
    private PushConsumer pushConsumer;

    /**
     * 消息监听器
     */
    private RocketMQMessageListener<Object> rocketMQMessageListener;

    /**
     * 消息消费者的配置
     */
    private RocketMQMessageConsumer rocketMQMessageConsumer;

    /**
     * 接入点
     */
    private String endpoints;

    /**
     * 消费者组
     */
    private String consumerGroup;

    /**
     * 消费线程数
     */
    private int consumptionThreadCount = 20;

    /**
     * 主题
     */
    String topic;

    /**
     * 过滤器类型
     */
    private FilterExpressionType filterType;

    /**
     * 过滤表达式
     */
    private String filterExpression;

    /**
     * 是否启用ssl
     */
    private boolean enableSsl;

    /**
     * 请求超时时间
     */
    private Duration requestTimeout;

    /**
     * 用户名
     */
    private String accessKey;

    /**
     * 用户密钥
     */
    private String secretKey;

    /**
     * 最大缓存消息数
     */
    private int maxCachedMessageCount = 1024;

    /**
     * 最大缓存消息大小
     */
    private int maxCacheMessageSizeInBytes = 67108864;

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public void stop(Runnable callback) {
        stop();
        callback.run();
    }

    @Override
    public void destroy() {
        stop();
    }

    @Override
    public void stop() {
        if (running) {
            if (Objects.nonNull(pushConsumer)) {
                try {
                    pushConsumer.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            running = false;
        }
    }

    @Override
    public void start() {
        if (running) {
            throw new IllegalStateException("container `" + name + "` already running.");
        }
        try {
            Asserts.notNull(rocketMQMessageConsumer, "Property 'rocketMQMessageConsumer' is required");
            Asserts.notEmpty(endpoints, "Property 'endpoints' is required");
            Asserts.notEmpty(consumerGroup, "Property 'consumerGroup' is required");
            Asserts.notEmpty(topic, "Property 'topic' is required");

            FilterExpression filterExpression = new FilterExpression(this.filterExpression, filterType);
            ClientConfiguration clientConfiguration = RocketMQUtils.createClientConfiguration(endpoints, enableSsl, accessKey,
                    secretKey, requestTimeout);

            PushConsumerBuilder pushConsumerBuilder = RocketMQUtils.getClientServiceProvider().newPushConsumerBuilder()
                    .setClientConfiguration(clientConfiguration)
                    .setConsumerGroup(consumerGroup)
                    .setSubscriptionExpressions(Maps.singletonMap(topic, filterExpression))
                    .setConsumptionThreadCount(consumptionThreadCount)
                    .setMaxCacheMessageCount(maxCachedMessageCount)
                    .setMaxCacheMessageSizeInBytes(maxCacheMessageSizeInBytes)
                    .setMessageListener(messageView -> {
                        try {
                            Object message = rocketMQMessageSerializer.deserialize(NioUtil.readBytes(messageView.getBody()));
                            return rocketMQMessageListener.consume(message, messageView);
                        } catch (Exception e) {
                            log.error("Message consumed exception endpoints = {} ,consumerGroup = {} ,topic = {}",
                                    endpoints, consumerGroup, topic, e);
                            throw new RuntimeException(e);
                        }
                    });
            pushConsumer = pushConsumerBuilder.build();
        } catch (ClientException e) {
            throw new RuntimeException(e);
        }
        running = true;
        log.info("running container: {}", this);
    }

    @Override
    public void setApplicationContext(@NotNull ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

}
