package cn.tmkit.mq.rocketmq5.boot.core;

import cn.tmkit.core.date.LocalDateTimes;
import cn.tmkit.core.lang.*;
import cn.tmkit.mq.rocketmq5.boot.serializer.RocketMQMessageSerializer;
import cn.tmkit.mq.rocketmq5.boot.util.RocketMQUtils;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.message.MessageBuilder;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.apache.rocketmq.client.java.impl.producer.SendReceiptImpl;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * RocketMQ Template for RocketMQ 5.x
 *
 * @author miles.tang
 * @version 0.0.1
 */
@Slf4j
@Getter
@Setter
public class RocketMQTemplate implements DisposableBean {

    /**
     * 生产者
     */
    private Producer producer;

    /**
     * 默认的主题
     */
    private String defaultTopic;

    /**
     * 消息转换器
     */
    private RocketMQMessageSerializer<Object> rocketMQMessageSerializer;

    /**
     * 异步发送线程池
     */
    private ThreadPoolTaskExecutor asyncSendThreadPoolTaskExecutor;

    // region 同步发送消息

    /**
     * 同步发送消息，发送到默认主题{@linkplain #defaultTopic}上
     *
     * @param tag  消息标签
     * @param body 消息内容
     * @return {@link SendResult}
     */
    public SendResult send(String tag, @NotNull Object body) {
        return send(tag, body, (Map<String, String>) null);
    }

    /**
     * 同步发送消息，发送到默认主题{@linkplain #defaultTopic}上
     *
     * @param tag        消息标签
     * @param body       消息内容
     * @param properties 自定义属性
     * @return {@link SendResult}
     */
    public SendResult send(String tag, @NotNull Object body, @Nullable Map<String, String> properties) {
        return send(tag, body, properties, null);
    }

    /**
     * 同步发送消息，发送到默认主题{@linkplain #defaultTopic}上
     *
     * @param tag  消息标签
     * @param body 消息内容
     * @param keys 消息标识
     * @return {@link SendResult}
     */
    public SendResult send(String tag, @NotNull Object body, @Nullable Collection<String> keys) {
        return send(tag, body, null, keys);
    }

    /**
     * 同步发送消息，发送到默认主题{@linkplain #defaultTopic}上
     *
     * @param tag  消息标签
     * @param body 消息内容
     * @param key  消息标识
     * @return {@link SendResult}
     */
    public SendResult send(String tag, @NotNull Object body, @Nullable String key) {
        return send(tag, body, null, Collections.singletonList(key));
    }

    /**
     * 同步发送消息，发送到默认主题{@linkplain #defaultTopic}上
     *
     * @param tag        消息标签
     * @param body       消息内容
     * @param properties 自定义属性
     * @param keys       消息标识
     * @return {@link SendResult}
     */
    public SendResult send(String tag, @NotNull Object body, @Nullable Map<String, String> properties,
                           @Nullable Collection<String> keys) {
        return send(defaultTopic, tag, body, properties, keys);
    }

    /**
     * 同步发送消息，发送到指定主题上
     *
     * @param topic 主题，不能为空
     * @param tag   消息标签
     * @param body  消息内容
     * @return {@link SendResult}
     */
    public SendResult send(@NotNull String topic, String tag, @NotNull Object body) {
        return send(topic, tag, body, (Map<String, String>) null);
    }

    /**
     * 同步发送消息，发送到指定主题上
     *
     * @param topic      主题
     * @param tag        消息标签
     * @param body       消息内容
     * @param properties 自定义属性
     * @return {@link SendResult}
     */
    public SendResult send(@NotNull String topic, String tag, @NotNull Object body, @Nullable Map<String, String> properties) {
        return send(topic, tag, body, properties, null);
    }

    /**
     * 同步发送消息，发送到指定主题上
     *
     * @param body 消息内容
     * @param keys 消息标识
     * @return {@link SendResult}
     */
    public SendResult send(@NotNull String topic, String tag, @NotNull Object body, @Nullable Collection<String> keys) {
        return send(topic, tag, body, null, keys);
    }

    /**
     * 同步发送消息，发送到指定主题上
     *
     * @param topic      主题
     * @param tag        消息标签
     * @param body       消息内容
     * @param properties 自定义属性
     * @param keys       消息标识
     * @return {@link SendResult}
     */
    public SendResult send(@NotNull String topic, @NotNull String tag, @NotNull Object body,
                           @Nullable Map<String, String> properties, @Nullable Collection<String> keys) {
        return doSyncSend(topic, tag, body, properties, keys, null);
    }

    // endregion

    // region 同步发送定时消息

    /**
     * 同步发定时送消息，发送到默认主题{@linkplain #defaultTopic}上
     *
     * @param tag          消息的标签
     * @param body         消息的内容
     * @param deliveryTime 分发的时间戳
     * @return {@link SendResult}
     */
    public SendResult sendSchedule(String tag, @NotNull Object body, long deliveryTime) {
        return sendSchedule(tag, body, deliveryTime, (Map<String, String>) null);
    }

    /**
     * 同步发定时送消息，发送到默认主题{@linkplain #defaultTopic}上
     *
     * @param tag          消息的标签
     * @param body         消息的内容
     * @param deliveryTime 分发的时间戳
     * @return {@link SendResult}
     */
    public SendResult sendSchedule(String tag, @NotNull Object body, @NotNull LocalDateTime deliveryTime) {
        return sendSchedule(tag, body, deliveryTime, (Map<String, String>) null);
    }

    /**
     * 同步发定时送消息，发送到默认主题{@linkplain #defaultTopic}上
     *
     * @param tag          消息的标签
     * @param body         消息的内容
     * @param deliveryTime 分发的时间戳
     * @return {@link SendResult}
     */
    public SendResult sendSchedule(String tag, @NotNull Object body, @NotNull Instant deliveryTime) {
        return sendSchedule(tag, body, deliveryTime, (Map<String, String>) null);
    }

    /**
     * 同步发定时送消息，发送到默认主题{@linkplain #defaultTopic}上
     *
     * @param tag          消息的标签
     * @param body         消息的内容
     * @param deliveryTime 分发的时间戳
     * @param properties   自定义属性
     * @return {@link SendResult}
     */
    public SendResult sendSchedule(String tag, @NotNull Object body, long deliveryTime, @Nullable Map<String, String> properties) {
        return sendSchedule(tag, body, deliveryTime, properties, null);
    }

    /**
     * 同步发定时送消息，发送到默认主题{@linkplain #defaultTopic}上
     *
     * @param tag          消息的标签
     * @param body         消息的内容
     * @param deliveryTime 分发的时间戳
     * @param properties   自定义属性
     * @return {@link SendResult}
     */
    public SendResult sendSchedule(String tag, @NotNull Object body, @NotNull LocalDateTime deliveryTime,
                                   @Nullable Map<String, String> properties) {
        return sendSchedule(tag, body, deliveryTime, properties, null);
    }

    /**
     * 同步发定时送消息，发送到默认主题{@linkplain #defaultTopic}上
     *
     * @param tag          消息的标签
     * @param body         消息的内容
     * @param deliveryTime 分发的时间戳
     * @param properties   自定义属性
     * @return {@link SendResult}
     */
    public SendResult sendSchedule(String tag, @NotNull Object body, @NotNull Instant deliveryTime,
                                   @Nullable Map<String, String> properties) {
        return sendSchedule(tag, body, deliveryTime, properties, null);
    }

    /**
     * 同步发定时送消息，发送到默认主题{@linkplain #defaultTopic}上
     *
     * @param tag          消息的标签
     * @param body         消息的内容
     * @param deliveryTime 分发的时间戳
     * @param key          消息标识
     * @return {@link SendResult}
     */
    public SendResult sendSchedule(String tag, @NotNull Object body, long deliveryTime, @Nullable String key) {
        return sendSchedule(tag, body, deliveryTime, null, Collections.singletonList(key));
    }

    /**
     * 同步发定时送消息，发送到默认主题{@linkplain #defaultTopic}上
     *
     * @param tag          消息的标签
     * @param body         消息的内容
     * @param deliveryTime 分发的时间戳
     * @param key          消息标识
     * @return {@link SendResult}
     */
    public SendResult sendSchedule(String tag, @NotNull Object body, @NotNull LocalDateTime deliveryTime, @Nullable String key) {
        return sendSchedule(tag, body, deliveryTime, null, Collections.singletonList(key));
    }

    /**
     * 同步发定时送消息，发送到默认主题{@linkplain #defaultTopic}上
     *
     * @param tag          消息的标签
     * @param body         消息的内容
     * @param deliveryTime 分发的时间戳
     * @param key          消息标识
     * @return {@link SendResult}
     */
    public SendResult sendSchedule(String tag, @NotNull Object body, @NotNull Instant deliveryTime, @Nullable String key) {
        return sendSchedule(tag, body, deliveryTime, null, Collections.singletonList(key));
    }

    /**
     * 同步发定时送消息，发送到默认主题{@linkplain #defaultTopic}上
     *
     * @param tag          消息的标签
     * @param body         消息的内容
     * @param deliveryTime 分发的时间戳
     * @param keys         消息标识
     * @return {@link SendResult}
     */
    public SendResult sendSchedule(String tag, @NotNull Object body, long deliveryTime, @Nullable Collection<String> keys) {
        return sendSchedule(tag, body, deliveryTime, null, keys);
    }

    /**
     * 同步发定时送消息，发送到默认主题{@linkplain #defaultTopic}上
     *
     * @param tag          消息的标签
     * @param body         消息的内容
     * @param deliveryTime 分发的时间戳
     * @param keys         消息标识
     * @return {@link SendResult}
     */
    public SendResult sendSchedule(String tag, @NotNull Object body, @NotNull LocalDateTime deliveryTime,
                                   @Nullable Collection<String> keys) {
        return sendSchedule(tag, body, deliveryTime, null, keys);
    }

    /**
     * 同步发定时送消息，发送到默认主题{@linkplain #defaultTopic}上
     *
     * @param tag          消息的标签
     * @param body         消息的内容
     * @param deliveryTime 分发的时间戳
     * @param keys         消息标识
     * @return {@link SendResult}
     */
    public SendResult sendSchedule(String tag, @NotNull Object body, @NotNull Instant deliveryTime,
                                   @Nullable Collection<String> keys) {
        return sendSchedule(tag, body, deliveryTime, null, keys);
    }

    /**
     * 同步发定时送消息，发送到默认主题{@linkplain #defaultTopic}上
     *
     * @param tag          消息的标签
     * @param body         消息的内容
     * @param deliveryTime 分发的时间戳
     * @param properties   自定义属性
     * @param keys         消息标识
     * @return {@link SendResult}
     */
    public SendResult sendSchedule(String tag, @NotNull Object body, long deliveryTime, @Nullable Map<String, String> properties,
                                   @Nullable Collection<String> keys) {
        return sendSchedule(defaultTopic, tag, body, deliveryTime, properties, keys);
    }

    /**
     * 同步发定时送消息，发送到默认主题{@linkplain #defaultTopic}上
     *
     * @param tag          消息的标签
     * @param body         消息的内容
     * @param deliveryTime 分发的时间戳
     * @param properties   自定义属性
     * @param keys         消息标识
     * @return {@link SendResult}
     */
    public SendResult sendSchedule(String tag, @NotNull Object body, @NotNull LocalDateTime deliveryTime,
                                   @Nullable Map<String, String> properties, @Nullable Collection<String> keys) {
        return sendSchedule(defaultTopic, tag, body, deliveryTime, properties, keys);
    }

    /**
     * 同步发定时送消息，发送到默认主题{@linkplain #defaultTopic}上
     *
     * @param tag          消息的标签
     * @param body         消息的内容
     * @param deliveryTime 分发的时间戳
     * @param properties   自定义属性
     * @param keys         消息标识
     * @return {@link SendResult}
     */
    public SendResult sendSchedule(String tag, @NotNull Object body, @NotNull Instant deliveryTime,
                                   @Nullable Map<String, String> properties, @Nullable Collection<String> keys) {
        return sendSchedule(defaultTopic, tag, body, deliveryTime, properties, keys);
    }

    /**
     * 同步发定时送消息，发送到指定主题上
     *
     * @param tag          消息的标签
     * @param body         消息的内容
     * @param deliveryTime 分发的时间戳
     * @return {@link SendResult}
     */
    public SendResult sendSchedule(@NotNull String topic, String tag, @NotNull Object body, long deliveryTime) {
        return sendSchedule(topic, tag, body, deliveryTime, (Map<String, String>) null);
    }

    /**
     * 同步发定时送消息，发送到指定主题上
     *
     * @param tag          消息的标签
     * @param body         消息的内容
     * @param deliveryTime 分发的时间戳
     * @return {@link SendResult}
     */
    public SendResult sendSchedule(@NotNull String topic, String tag, @NotNull Object body, @NotNull LocalDateTime deliveryTime) {
        return sendSchedule(topic, tag, body, deliveryTime, (Map<String, String>) null);
    }

    /**
     * 同步发定时送消息，发送到指定主题上
     *
     * @param tag          消息的标签
     * @param body         消息的内容
     * @param deliveryTime 分发的时间戳
     * @return {@link SendResult}
     */
    public SendResult sendSchedule(@NotNull String topic, String tag, @NotNull Object body, @NotNull Instant deliveryTime) {
        return sendSchedule(topic, tag, body, deliveryTime, (Map<String, String>) null);
    }

    /**
     * 同步发定时送消息，发送到指定主题上
     *
     * @param tag          消息的标签
     * @param body         消息的内容
     * @param deliveryTime 分发的时间戳
     * @param properties   自定义属性
     * @return {@link SendResult}
     */
    public SendResult sendSchedule(@NotNull String topic, String tag, @NotNull Object body, long deliveryTime,
                                   @Nullable Map<String, String> properties) {
        return sendSchedule(topic, tag, body, deliveryTime, properties, null);
    }

    /**
     * 同步发定时送消息，发送到指定主题上
     *
     * @param tag          消息的标签
     * @param body         消息的内容
     * @param deliveryTime 分发的时间戳
     * @param properties   自定义属性
     * @return {@link SendResult}
     */
    public SendResult sendSchedule(@NotNull String topic, String tag, @NotNull Object body, @NotNull LocalDateTime deliveryTime,
                                   @Nullable Map<String, String> properties) {
        return sendSchedule(topic, tag, body, deliveryTime, properties, null);
    }

    /**
     * 同步发定时送消息，发送到指定主题上
     *
     * @param tag          消息的标签
     * @param body         消息的内容
     * @param deliveryTime 分发的时间戳
     * @param properties   自定义属性
     * @return {@link SendResult}
     */
    public SendResult sendSchedule(@NotNull String topic, String tag, @NotNull Object body, @NotNull Instant deliveryTime,
                                   @Nullable Map<String, String> properties) {
        return sendSchedule(topic, tag, body, deliveryTime, properties, null);
    }

    /**
     * 同步发定时送消息，发送到指定主题上
     *
     * @param tag          消息的标签
     * @param body         消息的内容
     * @param deliveryTime 分发的时间戳
     * @param keys         消息标识
     * @return {@link SendResult}
     */
    public SendResult sendSchedule(@NotNull String topic, String tag, @NotNull Object body, long deliveryTime,
                                   @Nullable Collection<String> keys) {
        return sendSchedule(topic, tag, body, deliveryTime, null, keys);
    }

    /**
     * 同步发定时送消息，发送到指定主题上
     *
     * @param tag          消息的标签
     * @param body         消息的内容
     * @param deliveryTime 分发的时间戳
     * @param keys         消息标识
     * @return {@link SendResult}
     */
    public SendResult sendSchedule(@NotNull String topic, String tag, @NotNull Object body, @NotNull LocalDateTime deliveryTime,
                                   @Nullable Collection<String> keys) {
        return sendSchedule(topic, tag, body, deliveryTime, null, keys);
    }

    /**
     * 同步发定时送消息，发送到指定主题上
     *
     * @param tag          消息的标签
     * @param body         消息的内容
     * @param deliveryTime 分发的时间戳
     * @param keys         消息标识
     * @return {@link SendResult}
     */
    public SendResult sendSchedule(@NotNull String topic, String tag, @NotNull Object body, @NotNull Instant deliveryTime,
                                   @Nullable Collection<String> keys) {
        return sendSchedule(topic, tag, body, deliveryTime, null, keys);
    }

    /**
     * 同步发定时送消息，发送到指定主题上
     *
     * @param tag          消息的标签
     * @param body         消息的内容
     * @param deliveryTime 分发的时间戳
     * @param properties   自定义属性
     * @param keys         消息标识
     * @return {@link SendResult}
     */
    public SendResult sendSchedule(@NotNull String topic, String tag, @NotNull Object body, long deliveryTime,
                                   @Nullable Map<String, String> properties, @Nullable Collection<String> keys) {
        return doSyncSend(topic, tag, body, properties, keys, deliveryTime);
    }

    /**
     * 同步发定时送消息，发送到指定主题上
     *
     * @param tag          消息的标签
     * @param body         消息的内容
     * @param deliveryTime 分发的时间戳
     * @param properties   自定义属性
     * @param keys         消息标识
     * @return {@link SendResult}
     */
    public SendResult sendSchedule(@NotNull String topic, String tag, @NotNull Object body, @NotNull LocalDateTime deliveryTime,
                                   @Nullable Map<String, String> properties, @Nullable Collection<String> keys) {
        return doSyncSend(topic, tag, body, properties, keys, LocalDateTimes.toEpochMilli(deliveryTime));
    }

    /**
     * 同步发定时送消息，发送到指定主题上
     *
     * @param tag          消息的标签
     * @param body         消息的内容
     * @param deliveryTime 分发的时间戳
     * @param properties   自定义属性
     * @param keys         消息标识
     * @return {@link SendResult}
     */
    public SendResult sendSchedule(@NotNull String topic, String tag, @NotNull Object body, @NotNull Instant deliveryTime,
                                   @Nullable Map<String, String> properties, @Nullable Collection<String> keys) {
        return doSyncSend(topic, tag, body, properties, keys, deliveryTime.toEpochMilli());
    }

    // endregion

    // region 异步发送消息

    /**
     * 异步发送消息，发送到默认主题{@linkplain #defaultTopic}上
     *
     * @param body         消息内容
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSend(String tag, @NotNull Object body, SendCallback sendCallback) {
        asyncSend(tag, body, (Map<String, String>) null, sendCallback);
    }

    /**
     * 异步发送消息，发送到默认主题{@linkplain #defaultTopic}上
     *
     * @param body         消息内容
     * @param properties   自定义属性
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSend(String tag, @NotNull Object body, @Nullable Map<String, String> properties, SendCallback sendCallback) {
        asyncSend(tag, body, properties, null, sendCallback);
    }

    /**
     * 异步发送消息，发送到默认主题{@linkplain #defaultTopic}上
     *
     * @param body         消息内容
     * @param keys         消息标识
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSend(String tag, @NotNull Object body, @Nullable Collection<String> keys, SendCallback sendCallback) {
        asyncSend(tag, body, null, keys, sendCallback);
    }

    /**
     * 异步发送消息，发送到默认主题{@linkplain #defaultTopic}上
     *
     * @param body         消息内容.
     * @param properties   自定义属性
     * @param keys         消息标识
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSend(String tag, @NotNull Object body, @Nullable Map<String, String> properties,
                          @Nullable Collection<String> keys, SendCallback sendCallback) {
        asyncSend(defaultTopic, tag, body, properties, keys, sendCallback);
    }

    /**
     * 异步发送消息，发送到指定主题上
     *
     * @param body         消息内容
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSend(@NotNull String topic, String tag, @NotNull Object body, SendCallback sendCallback) {
        asyncSend(topic, tag, body, (Map<String, String>) null, sendCallback);
    }

    /**
     * 异步发送消息，发送到指定主题上
     *
     * @param body         消息内容
     * @param properties   自定义属性
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSend(@NotNull String topic, String tag, @NotNull Object body, @Nullable Map<String, String> properties,
                          SendCallback sendCallback) {
        asyncSend(topic, tag, body, properties, null, sendCallback);
    }

    /**
     * 异步发送消息，发送到指定主题上
     *
     * @param body         消息内容
     * @param keys         消息标识
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSend(@NotNull String topic, String tag, @NotNull Object body, @Nullable Collection<String> keys,
                          SendCallback sendCallback) {
        asyncSend(topic, tag, body, null, keys, sendCallback);
    }

    /**
     * 异步发送消息，发送到指定主题上
     *
     * @param body         消息内容.
     * @param properties   自定义属性
     * @param keys         消息标识
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSend(@NotNull String topic, @NotNull String tag, @NotNull Object body,
                          @Nullable Map<String, String> properties, @Nullable Collection<String> keys, SendCallback sendCallback) {
        doAsyncSend(topic, tag, body, properties, keys, null, sendCallback);
    }

    // endregion

    // region 异步发送定时消息

    /**
     * 异步发送消息，发送到默认主题{@linkplain #defaultTopic}上
     *
     * @param body         消息内容
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendSchedule(String tag, @NotNull Object body, long deliveryTime, SendCallback sendCallback) {
        asyncSendSchedule(tag, body, (Map<String, String>) null, deliveryTime, sendCallback);
    }

    /**
     * 异步发送消息，发送到默认主题{@linkplain #defaultTopic}上
     *
     * @param body         消息内容
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendSchedule(String tag, @NotNull Object body, @NotNull LocalDateTime deliveryTime, SendCallback sendCallback) {
        asyncSendSchedule(tag, body, (Map<String, String>) null, deliveryTime, sendCallback);
    }

    /**
     * 异步发送消息，发送到默认主题{@linkplain #defaultTopic}上
     *
     * @param body         消息内容
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendSchedule(String tag, @NotNull Object body, @NotNull Instant deliveryTime, SendCallback sendCallback) {
        asyncSendSchedule(tag, body, (Map<String, String>) null, deliveryTime, sendCallback);
    }

    /**
     * 异步发送消息，发送到默认主题{@linkplain #defaultTopic}上
     *
     * @param body         消息内容
     * @param properties   自定义属性
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendSchedule(String tag, @NotNull Object body, @Nullable Map<String, String> properties,
                                  long deliveryTime, SendCallback sendCallback) {
        asyncSendSchedule(tag, body, properties, null, deliveryTime, sendCallback);
    }

    /**
     * 异步发送消息，发送到默认主题{@linkplain #defaultTopic}上
     *
     * @param body         消息内容
     * @param properties   自定义属性
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendSchedule(String tag, @NotNull Object body, @Nullable Map<String, String> properties,
                                  @NotNull LocalDateTime deliveryTime, SendCallback sendCallback) {
        asyncSendSchedule(tag, body, properties, null, deliveryTime, sendCallback);
    }

    /**
     * 异步发送消息，发送到默认主题{@linkplain #defaultTopic}上
     *
     * @param body         消息内容
     * @param properties   自定义属性
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendSchedule(String tag, @NotNull Object body, @Nullable Map<String, String> properties,
                                  @NotNull Instant deliveryTime, SendCallback sendCallback) {
        asyncSendSchedule(tag, body, properties, null, deliveryTime, sendCallback);
    }

    /**
     * 异步发送消息，发送到默认主题{@linkplain #defaultTopic}上
     *
     * @param body         消息内容
     * @param keys         消息标识
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendSchedule(String tag, @NotNull Object body, @Nullable Collection<String> keys, long deliveryTime,
                                  SendCallback sendCallback) {
        asyncSendSchedule(tag, body, null, keys, deliveryTime, sendCallback);
    }

    /**
     * 异步发送消息，发送到默认主题{@linkplain #defaultTopic}上
     *
     * @param body         消息内容
     * @param keys         消息标识
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendSchedule(String tag, @NotNull Object body, @Nullable Collection<String> keys,
                                  @NotNull LocalDateTime deliveryTime, SendCallback sendCallback) {
        asyncSendSchedule(tag, body, null, keys, deliveryTime, sendCallback);
    }

    /**
     * 异步发送消息，发送到默认主题{@linkplain #defaultTopic}上
     *
     * @param body         消息内容
     * @param keys         消息标识
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendSchedule(String tag, @NotNull Object body, @Nullable Collection<String> keys,
                                  @NotNull Instant deliveryTime, SendCallback sendCallback) {
        asyncSendSchedule(tag, body, null, keys, deliveryTime, sendCallback);
    }

    /**
     * 异步发送消息，发送到默认主题{@linkplain #defaultTopic}上
     *
     * @param body         消息内容.
     * @param properties   自定义属性
     * @param keys         消息标识
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendSchedule(String tag, @NotNull Object body, @Nullable Map<String, String> properties,
                                  @Nullable Collection<String> keys, long deliveryTime, SendCallback sendCallback) {
        asyncSendSchedule(defaultTopic, tag, body, properties, keys, deliveryTime, sendCallback);
    }

    /**
     * 异步发送消息，发送到默认主题{@linkplain #defaultTopic}上
     *
     * @param body         消息内容.
     * @param properties   自定义属性
     * @param keys         消息标识
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendSchedule(String tag, @NotNull Object body, @Nullable Map<String, String> properties,
                                  @Nullable Collection<String> keys, @NotNull LocalDateTime deliveryTime, SendCallback sendCallback) {
        asyncSendSchedule(defaultTopic, tag, body, properties, keys, deliveryTime, sendCallback);
    }

    /**
     * 异步发送消息，发送到默认主题{@linkplain #defaultTopic}上
     *
     * @param body         消息内容.
     * @param properties   自定义属性
     * @param keys         消息标识
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendSchedule(String tag, @NotNull Object body, @Nullable Map<String, String> properties,
                                  @Nullable Collection<String> keys, @NotNull Instant deliveryTime, SendCallback sendCallback) {
        asyncSendSchedule(defaultTopic, tag, body, properties, keys, deliveryTime, sendCallback);
    }

    /**
     * 异步发送消息，发送到指定主题上
     *
     * @param body         消息内容
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendSchedule(@NotNull String topic, String tag, @NotNull Object body, long deliveryTime, SendCallback sendCallback) {
        asyncSendSchedule(topic, tag, body, (Map<String, String>) null, deliveryTime, sendCallback);
    }

    /**
     * 异步发送消息，发送到指定主题上
     *
     * @param body         消息内容
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendSchedule(@NotNull String topic, String tag, @NotNull Object body,
                                  @NotNull LocalDateTime deliveryTime, SendCallback sendCallback) {
        asyncSendSchedule(topic, tag, body, (Map<String, String>) null, deliveryTime, sendCallback);
    }

    /**
     * 异步发送消息，发送到指定主题上
     *
     * @param body         消息内容
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendSchedule(@NotNull String topic, String tag, @NotNull Object body,
                                  @NotNull Instant deliveryTime, SendCallback sendCallback) {
        asyncSendSchedule(topic, tag, body, (Map<String, String>) null, deliveryTime, sendCallback);
    }

    /**
     * 异步发送消息，发送到指定主题上
     *
     * @param body         消息内容
     * @param properties   自定义属性
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendSchedule(@NotNull String topic, String tag, @NotNull Object body, @Nullable Map<String, String> properties,
                                  long deliveryTime, SendCallback sendCallback) {
        asyncSendSchedule(topic, tag, body, properties, null, deliveryTime, sendCallback);
    }

    /**
     * 异步发送消息，发送到指定主题上
     *
     * @param body         消息内容
     * @param properties   自定义属性
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendSchedule(@NotNull String topic, String tag, @NotNull Object body, @Nullable Map<String, String> properties,
                                  @NotNull LocalDateTime deliveryTime, SendCallback sendCallback) {
        asyncSendSchedule(topic, tag, body, properties, null, deliveryTime, sendCallback);
    }

    /**
     * 异步发送消息，发送到指定主题上
     *
     * @param body         消息内容
     * @param properties   自定义属性
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendSchedule(@NotNull String topic, String tag, @NotNull Object body, @Nullable Map<String, String> properties,
                                  @NotNull Instant deliveryTime, SendCallback sendCallback) {
        asyncSendSchedule(topic, tag, body, properties, null, deliveryTime, sendCallback);
    }

    /**
     * 异步发送消息，发送到指定主题上
     *
     * @param body         消息内容
     * @param keys         消息标识
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendSchedule(@NotNull String topic, String tag, @NotNull Object body, @Nullable Collection<String> keys,
                                  long deliveryTime, SendCallback sendCallback) {
        asyncSendSchedule(topic, tag, body, null, keys, deliveryTime, sendCallback);
    }

    /**
     * 异步发送消息，发送到指定主题上
     *
     * @param body         消息内容
     * @param keys         消息标识
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendSchedule(@NotNull String topic, String tag, @NotNull Object body, @Nullable Collection<String> keys,
                                  @NotNull LocalDateTime deliveryTime, SendCallback sendCallback) {
        asyncSendSchedule(topic, tag, body, null, keys, deliveryTime, sendCallback);
    }

    /**
     * 异步发送消息，发送到指定主题上
     *
     * @param body         消息内容
     * @param keys         消息标识
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendSchedule(@NotNull String topic, String tag, @NotNull Object body, @Nullable Collection<String> keys,
                                  @NotNull Instant deliveryTime, SendCallback sendCallback) {
        asyncSendSchedule(topic, tag, body, null, keys, deliveryTime, sendCallback);
    }

    /**
     * 异步发送消息，发送到指定主题上
     *
     * @param body         消息内容.
     * @param properties   自定义属性
     * @param keys         消息标识
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendSchedule(@Nullable String topic, @NotNull String tag, @NotNull Object body,
                                  @Nullable Map<String, String> properties, @Nullable Collection<String> keys,
                                  long deliveryTime, @Nullable SendCallback sendCallback) {
        doAsyncSend(topic, tag, body, properties, keys, deliveryTime, sendCallback);
    }

    /**
     * 异步发送消息，发送到指定主题上
     *
     * @param body         消息内容.
     * @param properties   自定义属性
     * @param keys         消息标识
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendSchedule(@Nullable String topic, @NotNull String tag, @NotNull Object body,
                                  @Nullable Map<String, String> properties, @Nullable Collection<String> keys,
                                  @NotNull LocalDateTime deliveryTime, @Nullable SendCallback sendCallback) {
        doAsyncSend(topic, tag, body, properties, keys, LocalDateTimes.toEpochMilli(deliveryTime), sendCallback);
    }

    /**
     * 异步发送消息，发送到指定主题上
     *
     * @param body         消息内容.
     * @param properties   自定义属性
     * @param keys         消息标识
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendSchedule(@Nullable String topic, @NotNull String tag, @NotNull Object body,
                                  @Nullable Map<String, String> properties, @Nullable Collection<String> keys,
                                  @NotNull Instant deliveryTime, @Nullable SendCallback sendCallback) {
        doAsyncSend(topic, tag, body, properties, keys, deliveryTime.toEpochMilli(), sendCallback);
    }

    // endregion

    // region 异步发送延迟消息

    /**
     * 异步发送延迟消息，发送到默认主题{@linkplain #defaultTopic}上
     *
     * @param tag          标签
     * @param body         消息内容
     * @param duration     延迟时长，最大为24小时
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendDelay(String tag, @NotNull Object body, @NotNull Duration duration, SendCallback sendCallback) {
        asyncSendDelay(tag, body, (Map<String, String>) null, duration, sendCallback);
    }

    /**
     * 异步发送延迟消息，发送到默认主题{@linkplain #defaultTopic}上
     *
     * @param tag          标签
     * @param body         消息内容
     * @param seconds      延迟的秒数，最大为24小时的秒
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendDelaySeconds(String tag, @NotNull Object body, int seconds, SendCallback sendCallback) {
        asyncSendDelay(tag, body, (Map<String, String>) null, Duration.ofSeconds(seconds), sendCallback);
    }

    /**
     * 异步发送消息，发送到默认主题{@linkplain #defaultTopic}上
     *
     * @param tag          标签
     * @param body         消息内容
     * @param minutes      延迟的分钟，最大为24小时的分钟
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendDelayMinutes(String tag, @NotNull Object body, int minutes, SendCallback sendCallback) {
        asyncSendDelay(tag, body, (Map<String, String>) null, Duration.ofMinutes(minutes), sendCallback);
    }

    /**
     * 异步发送消息，发送到默认主题{@linkplain #defaultTopic}上
     *
     * @param body         消息内容
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendDelayHours(String tag, @NotNull Object body, int hours, SendCallback sendCallback) {
        asyncSendDelay(tag, body, (Map<String, String>) null, Duration.ofHours(hours), sendCallback);
    }

    /**
     * 异步发送消息，发送到默认主题{@linkplain #defaultTopic}上
     *
     * @param tag          标签
     * @param body         消息内容
     * @param properties   自定义属性
     * @param duration     延迟时长，最大为24小时
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendDelay(String tag, @NotNull Object body, @Nullable Map<String, String> properties,
                               Duration duration, SendCallback sendCallback) {
        asyncSendDelay(tag, body, properties, null, duration, sendCallback);
    }

    /**
     * 异步发送消息，发送到默认主题{@linkplain #defaultTopic}上
     *
     * @param body         消息内容
     * @param properties   自定义属性
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendDelaySeconds(String tag, @NotNull Object body, @Nullable Map<String, String> properties,
                                      int seconds, SendCallback sendCallback) {
        asyncSendDelay(tag, body, properties, null, Duration.ofSeconds(seconds), sendCallback);
    }

    /**
     * 异步发送消息，发送到默认主题{@linkplain #defaultTopic}上
     *
     * @param body         消息内容
     * @param properties   自定义属性
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendDelayMinutes(String tag, @NotNull Object body, @Nullable Map<String, String> properties,
                                      int minutes, SendCallback sendCallback) {
        asyncSendDelay(tag, body, properties, null, Duration.ofMinutes(minutes), sendCallback);
    }

    /**
     * 异步发送消息，发送到默认主题{@linkplain #defaultTopic}上
     *
     * @param body         消息内容
     * @param properties   自定义属性
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendDelayHours(String tag, @NotNull Object body, @Nullable Map<String, String> properties,
                                    int hours, SendCallback sendCallback) {
        asyncSendDelay(tag, body, properties, null, Duration.ofHours(hours), sendCallback);
    }

    /**
     * 异步发送消息，发送到默认主题{@linkplain #defaultTopic}上
     *
     * @param tag          标签
     * @param body         消息内容
     * @param keys         消息标识
     * @param duration     延迟时长，最大为24小时
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendDelay(String tag, @NotNull Object body, @Nullable Collection<String> keys, @NotNull Duration duration,
                               SendCallback sendCallback) {
        asyncSendDelay(tag, body, null, keys, duration, sendCallback);
    }

    /**
     * 异步发送消息，发送到默认主题{@linkplain #defaultTopic}上
     *
     * @param body         消息内容
     * @param keys         消息标识
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendDelaySeconds(String tag, @NotNull Object body, @Nullable Collection<String> keys,
                                      int seconds, SendCallback sendCallback) {
        asyncSendDelay(tag, body, null, keys, Duration.ofSeconds(seconds), sendCallback);
    }

    /**
     * 异步发送消息，发送到默认主题{@linkplain #defaultTopic}上
     *
     * @param body         消息内容
     * @param keys         消息标识
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendDelayMinutes(String tag, @NotNull Object body, @Nullable Collection<String> keys,
                                      int minutes, SendCallback sendCallback) {
        asyncSendDelay(tag, body, null, keys, Duration.ofMinutes(minutes), sendCallback);
    }

    /**
     * 异步发送消息，发送到默认主题{@linkplain #defaultTopic}上
     *
     * @param body         消息内容
     * @param keys         消息标识
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendDelayHours(String tag, @NotNull Object body, @Nullable Collection<String> keys,
                                    int hours, SendCallback sendCallback) {
        asyncSendDelay(tag, body, null, keys, Duration.ofHours(hours), sendCallback);
    }

    /**
     * 异步发送消息，发送到默认主题{@linkplain #defaultTopic}上
     *
     * @param tag          标签
     * @param body         消息内容.
     * @param properties   自定义属性
     * @param keys         消息标识
     * @param duration     延迟时长，最大为24小时
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendDelay(String tag, @NotNull Object body, @Nullable Map<String, String> properties,
                               @Nullable Collection<String> keys, @NotNull Duration duration, SendCallback sendCallback) {
        asyncSendDelay(defaultTopic, tag, body, properties, keys, duration, sendCallback);
    }

    /**
     * 异步发送消息，发送到默认主题{@linkplain #defaultTopic}上
     *
     * @param body         消息内容.
     * @param properties   自定义属性
     * @param keys         消息标识
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendDelaySeconds(String tag, @NotNull Object body, @Nullable Map<String, String> properties,
                                      @Nullable Collection<String> keys, int seconds, SendCallback sendCallback) {
        asyncSendDelay(defaultTopic, tag, body, properties, keys, Duration.ofSeconds(seconds), sendCallback);
    }

    /**
     * 异步发送消息，发送到默认主题{@linkplain #defaultTopic}上
     *
     * @param body         消息内容.
     * @param properties   自定义属性
     * @param keys         消息标识
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendDelayMinutes(String tag, @NotNull Object body, @Nullable Map<String, String> properties,
                                      @Nullable Collection<String> keys, int minutes, SendCallback sendCallback) {
        asyncSendDelay(defaultTopic, tag, body, properties, keys, Duration.ofMinutes(minutes), sendCallback);
    }

    /**
     * 异步发送消息，发送到默认主题{@linkplain #defaultTopic}上
     *
     * @param body         消息内容.
     * @param properties   自定义属性
     * @param keys         消息标识
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendDelayHours(String tag, @NotNull Object body, @Nullable Map<String, String> properties,
                                    @Nullable Collection<String> keys, int hours, SendCallback sendCallback) {
        asyncSendDelay(defaultTopic, tag, body, properties, keys, Duration.ofHours(hours), sendCallback);
    }

    /**
     * 异步发送消息，发送到指定主题上
     *
     * @param body         消息内容
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendDelay(@NotNull String topic, String tag, @NotNull Object body, @NotNull Duration duration,
                               SendCallback sendCallback) {
        asyncSendDelay(topic, tag, body, (Map<String, String>) null, duration, sendCallback);
    }

    /**
     * 异步发送消息，发送到指定主题上
     *
     * @param body         消息内容
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendDelaySeconds(@NotNull String topic, String tag, @NotNull Object body, int seconds, SendCallback sendCallback) {
        asyncSendDelay(topic, tag, body, (Map<String, String>) null, Duration.ofSeconds(seconds), sendCallback);
    }

    /**
     * 异步发送消息，发送到指定主题上
     *
     * @param body         消息内容
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendDelayMinutes(@NotNull String topic, String tag, @NotNull Object body, int minutes, SendCallback sendCallback) {
        asyncSendDelay(topic, tag, body, (Map<String, String>) null, Duration.ofMinutes(minutes), sendCallback);
    }

    /**
     * 异步发送消息，发送到指定主题上
     *
     * @param body         消息内容
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendDelayHours(@NotNull String topic, String tag, @NotNull Object body, int hours, SendCallback sendCallback) {
        asyncSendDelay(topic, tag, body, (Map<String, String>) null, Duration.ofHours(hours), sendCallback);
    }

    /**
     * 异步发送消息，发送到指定主题上
     *
     * @param body         消息内容
     * @param properties   自定义属性
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendDelay(@NotNull String topic, String tag, @NotNull Object body, @Nullable Map<String, String> properties,
                               @NotNull Duration duration, SendCallback sendCallback) {
        asyncSendDelay(topic, tag, body, properties, null, duration, sendCallback);
    }

    /**
     * 异步发送消息，发送到指定主题上
     *
     * @param body         消息内容
     * @param properties   自定义属性
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendDelaySeconds(@NotNull String topic, String tag, @NotNull Object body, @Nullable Map<String, String> properties,
                                      int seconds, SendCallback sendCallback) {
        asyncSendDelay(topic, tag, body, properties, null, Duration.ofSeconds(seconds), sendCallback);
    }

    /**
     * 异步发送消息，发送到指定主题上
     *
     * @param body         消息内容
     * @param properties   自定义属性
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendDelayMinutes(@NotNull String topic, String tag, @NotNull Object body, @Nullable Map<String, String> properties,
                                      int minutes, SendCallback sendCallback) {
        asyncSendDelay(topic, tag, body, properties, null, Duration.ofMinutes(minutes), sendCallback);
    }

    /**
     * 异步发送消息，发送到指定主题上
     *
     * @param body         消息内容
     * @param properties   自定义属性
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendDelayHours(@NotNull String topic, String tag, @NotNull Object body, @Nullable Map<String, String> properties,
                                    int hours, SendCallback sendCallback) {
        asyncSendDelay(topic, tag, body, properties, null, Duration.ofHours(hours), sendCallback);
    }

    /**
     * 异步发送消息，发送到指定主题上
     *
     * @param body         消息内容
     * @param keys         消息标识
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendDelay(@NotNull String topic, String tag, @NotNull Object body, @Nullable Collection<String> keys,
                               @NotNull Duration duration, SendCallback sendCallback) {
        asyncSendDelay(topic, tag, body, null, keys, duration, sendCallback);
    }

    /**
     * 异步发送消息，发送到指定主题上
     *
     * @param body         消息内容
     * @param keys         消息标识
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendDelaySeconds(@NotNull String topic, String tag, @NotNull Object body, @Nullable Collection<String> keys,
                                      int seconds, SendCallback sendCallback) {
        asyncSendDelay(topic, tag, body, null, keys, Duration.ofSeconds(seconds), sendCallback);
    }

    /**
     * 异步发送消息，发送到指定主题上
     *
     * @param body         消息内容
     * @param keys         消息标识
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendDelayMinutes(@NotNull String topic, String tag, @NotNull Object body, @Nullable Collection<String> keys,
                                      int minutes, SendCallback sendCallback) {
        asyncSendDelay(topic, tag, body, null, keys, Duration.ofMinutes(minutes), sendCallback);
    }

    /**
     * 异步发送消息，发送到指定主题上
     *
     * @param body         消息内容
     * @param keys         消息标识
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendDelayHours(@NotNull String topic, String tag, @NotNull Object body, @Nullable Collection<String> keys,
                                    int hours, SendCallback sendCallback) {
        asyncSendDelay(topic, tag, body, null, keys, Duration.ofHours(hours), sendCallback);
    }

    /**
     * 异步发送消息，发送到指定主题上
     *
     * @param body         消息内容.
     * @param properties   自定义属性
     * @param keys         消息标识
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendDelay(@Nullable String topic, @NotNull String tag, @NotNull Object body,
                               @Nullable Map<String, String> properties, @Nullable Collection<String> keys,
                               @NotNull Duration duration, @Nullable SendCallback sendCallback) {
        asyncSendSchedule(topic, tag, body, properties, keys, LocalDateTimes.now().plus(duration), sendCallback);
    }

    /**
     * 异步发送消息，发送到指定主题上
     *
     * @param body         消息内容.
     * @param properties   自定义属性
     * @param keys         消息标识
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendDelaySeconds(@Nullable String topic, @NotNull String tag, @NotNull Object body,
                                      @Nullable Map<String, String> properties, @Nullable Collection<String> keys,
                                      int seconds, @Nullable SendCallback sendCallback) {
        asyncSendSchedule(topic, tag, body, properties, keys, LocalDateTimes.now().plusSeconds(seconds), sendCallback);
    }

    /**
     * 异步发送消息，发送到指定主题上
     *
     * @param body         消息内容.
     * @param properties   自定义属性
     * @param keys         消息标识
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendDelayMinutes(@Nullable String topic, @NotNull String tag, @NotNull Object body,
                                      @Nullable Map<String, String> properties, @Nullable Collection<String> keys,
                                      int minutes, @Nullable SendCallback sendCallback) {
        asyncSendSchedule(topic, tag, body, properties, keys, LocalDateTimes.now().plusMinutes(minutes), sendCallback);
    }

    /**
     * 异步发送消息，发送到指定主题上
     *
     * @param body         消息内容.
     * @param properties   自定义属性
     * @param keys         消息标识
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendDelayHours(@Nullable String topic, @NotNull String tag, @NotNull Object body,
                                    @Nullable Map<String, String> properties, @Nullable Collection<String> keys,
                                    int hours, @Nullable SendCallback sendCallback) {
        asyncSendSchedule(topic, tag, body, properties, keys, LocalDateTimes.now().plusHours(hours), sendCallback);
    }

    // endregion

    /**
     * 发送消息
     *
     * @param topic        自定义的主题
     * @param tag          标签
     * @param body         消息内容
     * @param properties   自定义的属性
     * @param keys         消息标识
     * @param deliveryTime 分发时间
     * @return 发送结果
     */
    private SendResult doSyncSend(String topic, String tag, Object body, Map<String, String> properties, Collection<String> keys, Long deliveryTime) {
        try {
            SendReceiptImpl sendReceiptImpl = (SendReceiptImpl) producer.send(createRocketMQMessage(topic, tag, body, properties, keys, deliveryTime));
            return new SendResult(sendReceiptImpl.getMessageId(), sendReceiptImpl.getTransactionId(), sendReceiptImpl.getMessageQueue(),
                    sendReceiptImpl.getOffset());
        } catch (Exception e) {
            log.error("send request message failed. topic = {} ,tag = {} ,body = {} ", topic, tag, body, e);
            throw new MessageException(e.getMessage(), e);
        }
    }

    /**
     * 异步发送消息
     *
     * @param topic        主题
     * @param tag          标签
     * @param body         消息内容
     * @param properties   自定义属性
     * @param keys         消息标识
     * @param sendCallback {@link SendCallback}
     */
    private void doAsyncSend(String topic, String tag, Object body, Map<String, String> properties,
                             Collection<String> keys, Long deliveryTime, SendCallback sendCallback) {
        CompletableFuture<SendReceipt> completableFuture = producer.sendAsync(createRocketMQMessage(topic, tag, body, properties, keys, deliveryTime));
        if (sendCallback != null) {
            completableFuture.whenCompleteAsync((sendReceipt, throwable) -> {
                SendReceiptImpl sendReceiptImpl = (SendReceiptImpl) sendReceipt;
                if (throwable != null) {
                    sendCallback.onException(throwable);
                } else {
                    sendCallback.onSuccess(new SendResult(sendReceiptImpl.getMessageId(), sendReceiptImpl.getTransactionId(),
                            sendReceiptImpl.getMessageQueue(), sendReceiptImpl.getOffset()));
                }
            }, asyncSendThreadPoolTaskExecutor);
        }
    }

    /**
     * 创建消息
     *
     * @param topic        指定的主题
     * @param tag          消息标签
     * @param body         消息内容
     * @param properties   自定义属性
     * @param keys         消息标识
     * @param deliveryTime 分发的时间
     * @return {@linkplain Message}
     */
    private Message createRocketMQMessage(String topic, String tag, Object body, Map<String, String> properties,
                                          Collection<String> keys, Long deliveryTime) {
        MessageBuilder messageBuilder = RocketMQUtils.getClientServiceProvider().newMessageBuilder()
                .setTopic(topic)
                .setTag(tag)
                .setBody(getRocketMQMessageSerializer().serialize(body));
        if (Maps.isNotEmpty(properties)) {
            properties.forEach(messageBuilder::addProperty);
        }
        if (CollectionUtil.isNotEmpty(keys)) {
            keys.forEach(messageBuilder::setKeys);
        }
        if (ObjectUtil.nonNull(deliveryTime)) {
            if (deliveryTime < System.currentTimeMillis()) {
                throw new MessageException("deliveryTime must be greater than current time");
            }
            messageBuilder.setDeliveryTimestamp(deliveryTime);
        }
        return messageBuilder.build();
    }

    @Override
    public void destroy() {
        if (Objects.nonNull(producer)) {
            try {
                producer.close();
            } catch (IOException e) {
                log.warn("Destroy Producer occupy exception", e);
                // ignore
            }
        }
        if (Objects.nonNull(asyncSendThreadPoolTaskExecutor)) {
            asyncSendThreadPoolTaskExecutor.destroy();
        }
    }

}
