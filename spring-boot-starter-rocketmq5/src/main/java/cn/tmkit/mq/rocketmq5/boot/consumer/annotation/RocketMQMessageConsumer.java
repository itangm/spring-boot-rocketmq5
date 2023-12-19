package cn.tmkit.mq.rocketmq5.boot.consumer.annotation;

import org.apache.rocketmq.client.apis.consumer.FilterExpressionType;

import java.lang.annotation.*;

/**
 * RocketMQ的消费者
 *
 * @author ming.tang
 * @version 0.0.1
 * @date 2023-12-18
 */
@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface RocketMQMessageConsumer {

    /**
     * RocketMQ端点，RocketMQ 5.x端点是Proxy的地址，多个Proxy采用因为分号分割，格式如下：`host:port;host:port`。
     */
    String endpoints() default "${spring.rocketmq.endpoints:}";

    /**
     * 消费者监听的主题名<br>
     * <del>如果为空则查找配置<code>${spring.rocketmq.push-consumer.topics.类名}</code></del>
     */
    String topic();

    /**
     * 过滤表达式类型
     */
    FilterExpressionType filterExpressionType() default FilterExpressionType.TAG;

    /**
     * 过滤表达式
     */
    String filterExpression() default "*";

    /**
     * 消费者分组的名称，用于区分不同的消费者分组。集群内全局唯一。更为详细请查看<a href="https://rocketmq.apache.org/zh/docs/domainModel/07consumergroup">消费者分组</a><br>
     * <p>
     * <del>
     * 如果为空则查找配置<code>${spring.rocketmq.push-consumer.consumer-groups.类名}</code>
     * </del>
     * <del>
     * 如果配置项未配置，默认生成的规则：topic + "-" + 类名
     * </del>
     * </p>
     */
    String group();

    /**
     * 线程池大小
     */
    String consumptionThreadCount() default "16";

    /**
     * 是否使用SSL
     */
    String enableSsl() default "off";

    /**
     * 请求超时时间，单位秒；默认3s
     */
    int requestTimeout() default 3;

    /**
     * 账户名
     */
    String accessKey() default "${spring.rocketmq.push-consumer.access-key:}";

    /**
     * 账户密钥
     */
    String secretKey() default "${spring.rocketmq.push-consumer.secret-key:}";

    /**
     * 最大缓存消息数量
     */
    int maxCachedMessageCount() default 1024;

    /**
     * 最大缓存消息大小，默认时4M
     */
    int maxCacheMessageSizeInBytes() default 4194304;

}
