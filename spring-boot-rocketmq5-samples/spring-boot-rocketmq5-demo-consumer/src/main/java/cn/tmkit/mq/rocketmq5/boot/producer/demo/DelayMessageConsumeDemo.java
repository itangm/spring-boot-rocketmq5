package cn.tmkit.mq.rocketmq5.boot.producer.demo;

import cn.tmkit.mq.rocketmq5.boot.consumer.annotation.RocketMQMessageConsumer;
import cn.tmkit.mq.rocketmq5.boot.consumer.listener.RocketMQMessageListener;
import cn.tmkit.mq.rocketmq5.boot.demo.vo.OrderVO;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.springframework.stereotype.Component;

/**
 * 延迟消息的消费
 *
 * @author ming.tang
 * @version 0.0.1
 * @date 2023-12-19
 */
@Slf4j
@Component
@RocketMQMessageConsumer(
        topic = "${spring.rocketmq.consumers.DelayMessageConsumeDemo.topic}",
        filterExpression = "${spring.rocketmq.consumers.DelayMessageConsumeDemo.filter-expression}",
        group = "${spring.rocketmq.consumers.DelayMessageConsumeDemo.group}"
)
public class DelayMessageConsumeDemo implements RocketMQMessageListener<OrderVO> {

    /**
     * 消费消息
     *
     * @param message 解析后的消息
     * @param mv      消息视图
     */
    @Override
    public ConsumeResult consume(OrderVO message, MessageView mv) {
        String id = mv.getMessageId().toString();
        log.info("Printing message. id = {} ,body = {} ,properties = {} ,keys = {}", id, message, mv.getProperties(),
                mv.getKeys());
        return ConsumeResult.SUCCESS;
    }

}
