package cn.tmkit.mq.rocketmq5.boot.core;

import lombok.Getter;
import lombok.ToString;
import org.apache.rocketmq.client.apis.message.MessageId;
import org.apache.rocketmq.client.java.route.MessageQueueImpl;

/**
 * 发送结果
 *
 * @author ming.tang
 * @version 0.0.1
 * @date 2023-12-18
 */
@Getter
@ToString(exclude = "messageId")
public class SendResult {

    /**
     * 发送消息的唯一标识
     */
    private final String msgId;

    /**
     * 发送消息的唯一标识
     */
    private final MessageId messageId;

    /**
     * 事务消息的唯一标识
     */
    private final String transactionId;

    /**
     * 发送消息的消息队列
     */
    private final MessageQueueImpl messageQueue;

    /**
     * 发送消息的偏移量
     */
    private final long offset;

    public SendResult(final MessageId messageId, final String transactionId, final MessageQueueImpl messageQueue, final long offset) {
        this.messageId = messageId;
        this.msgId = messageId.toString();
        this.transactionId = transactionId;
        this.messageQueue = messageQueue;
        this.offset = offset;
    }



}
