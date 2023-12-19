package cn.tmkit.mq.rocketmq5.boot.autoconfigure;

import lombok.*;
import lombok.experimental.SuperBuilder;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

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
    private Producer producer;

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
         * 绑定的主题列表
         */
        private List<String> bindTopics;

        /**
         * 默认的主题，如果没有指定消息的topic，则会使用默认的主题。
         * 如果不配置则采用{@linkplain #bindTopics}第一个主题作为默认主题
         */
        private String defaultTopic;

        /**
         * 发送消息超时时间，单位毫秒
         */
        private Integer requestTimeout = 3000;

        /**
         * 重试次数，默认为3
         */
        private int maxAttempts = 3;

    }

}
