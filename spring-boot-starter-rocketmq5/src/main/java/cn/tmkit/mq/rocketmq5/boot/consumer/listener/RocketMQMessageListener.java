package cn.tmkit.mq.rocketmq5.boot.consumer.listener;

import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.message.MessageView;

/**
 * RocketMQ的消费者
 *
 * @author ming.tang
 * @version 0.0.1
 * @date 2023-12-18
 */
public interface RocketMQMessageListener<T> {

    /**
     * 消费消息
     *
     * @param message 解析后的消息
     * @param mv      消息视图
     */
    ConsumeResult consume(T message, MessageView mv);

}
