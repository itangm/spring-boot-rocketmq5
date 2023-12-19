package cn.tmkit.mq.rocketmq5.boot.consumer.listener;

import org.springframework.beans.factory.DisposableBean;

/**
 * 处理带有注解{@linkplain cn.tmkit.mq.rocketmq5.boot.consumer.annotation.RocketMQMessageConsumer}的bean，
 *
 * @author ming.tang
 * @version 0.0.1
 * @date 2023-12-18
 */
public interface RocketMQListenerContainer extends DisposableBean {

}
