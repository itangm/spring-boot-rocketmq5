package cn.tmkit.mq.rocketmq5.boot.autoconfigure;

import lombok.*;
import lombok.experimental.SuperBuilder;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

import java.util.List;
import java.util.Map;

/**
 * RocketMQ的配置
 *
 * @author ming.tang
 * @version 0.0.1
 * @date 2023-12-18
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
@ConfigurationProperties(prefix = RocketMQProperties.PREFIX)
public class RocketMQProperties {

    /**
     * The prefix of rocketmq properties.
     */
    public static final String PREFIX = "spring.rocketmq";

    /**
     * RocketMQ端点，RocketMQ 5.x端点是Proxy的地址，多个Proxy采用因为分号分割，格式如下：`host:port;host:port`。
     */
    private String endpoints;

    /**
     * 账户名
     */
    private String accessKey;

    /**
     * 账户密钥
     */
    private String secretKey;

    /**
     * 是否开启ssl
     */
    private boolean enableSsl;

    /**
     * RocketMQ连接超时时间，单位毫秒；默认3秒
     */
    private int requestTimeout = 3000;

    /**
     * 生产者的配置
     */
    @NestedConfigurationProperty
    private Producer producer;

    /**
     * 消费者的配置，该配置仅用于提示的，系统未作处理
     */
    private Map<String, PushConsumer> consumers;

    /**
     * 生产者的配置
     */
    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @ToString
    @SuperBuilder
    public static class Producer {

        /**
         * 账户名
         */
        private String accessKey;

        /**
         * 账户密钥
         */
        private String secretKey;

        /**
         * 默认的普通主题，发送普通消息默认使用该主题
         */
        private String defaultNormalTopic;

        /**
         * 默认的延时消息主题，发送延时/定时消息默认使用该主题
         */
        private String defaultDelayTopic;

        /**
         * 额外的预绑定主题列表，<a href="https://rocketmq.apache.org/zh/docs/domainModel/04producer#%E5%86%85%E9%83%A8%E5%B1%9E%E6%80%A7">官方建议提前绑定</a>
         */
        private List<String> extBindTopics;

        /**
         * 发送消息超时时间，单位毫秒
         */
        private Integer requestTimeout = 3000;

        /**
         * 重试次数，默认为3
         */
        private int maxAttempts = 3;

    }

    /**
     * 消费者的配置
     */
    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @ToString
    @SuperBuilder
    public static class PushConsumer {

        /**
         * 账户名
         */
        private String accessKey;

        /**
         * 账户密钥
         */
        private String secretKey;

        /**
         * 消费者订阅的主题
         */
        private String topic;

        /**
         * 消费者的消费组
         */
        private String group;

        /**
         * 消费者订阅的过滤表达式
         */
        private String filterExpression;

    }


}
