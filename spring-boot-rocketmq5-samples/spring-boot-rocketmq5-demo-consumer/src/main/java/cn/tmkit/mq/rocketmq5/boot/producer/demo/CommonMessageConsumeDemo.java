package cn.tmkit.mq.rocketmq5.boot.producer.demo;

import cn.tmkit.mq.rocketmq5.boot.consumer.annotation.RocketMQMessageConsumer;
import cn.tmkit.mq.rocketmq5.boot.consumer.listener.RocketMQMessageListener;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.springframework.stereotype.Component;

/**
 * 常规消息的消费
 *
 * @author ming.tang
 * @version 0.0.1
 * @date 2023-12-19
 */
@Slf4j
@Component
@RocketMQMessageConsumer(
        filterExpression = "tag-common",
        consumerGroup = "${spring.rocketmq.push-consumer.consumer-group.common}"
)
public class CommonMessageConsumeDemo implements RocketMQMessageListener<String> {

    /**
     * 消费消息
     *
     * @param message 解析后的消息
     * @param mv      消息视图
     */
    @Override
    public ConsumeResult consume(String message, MessageView mv) {
        String id = mv.getMessageId().toString();
        log.info("Printing message. id = {} ,body = {} ,properties = {} ,keys = {}", id, message, mv.getProperties(),
                mv.getKeys());
        return ConsumeResult.SUCCESS;
    }

}
