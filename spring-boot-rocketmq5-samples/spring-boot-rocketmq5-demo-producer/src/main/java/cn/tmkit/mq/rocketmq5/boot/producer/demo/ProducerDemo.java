package cn.tmkit.mq.rocketmq5.boot.producer.demo;

import cn.tmkit.core.date.LocalDateTimes;
import cn.tmkit.core.lang.Maps;
import cn.tmkit.core.lang.RandomUtil;
import cn.tmkit.mq.rocketmq5.boot.autoconfigure.RocketMQProperties;
import cn.tmkit.mq.rocketmq5.boot.core.RocketMQTemplate;
import cn.tmkit.mq.rocketmq5.boot.core.SendCallback;
import cn.tmkit.mq.rocketmq5.boot.core.SendResult;
import cn.tmkit.mq.rocketmq5.boot.demo.vo.OrderVO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.Map;

/**
 * @author ming.tang
 * @version 0.0.1
 * @date 2023-12-19
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ProducerDemo implements CommandLineRunner {

    private final RocketMQTemplate rocketMQTemplate;
    private final RocketMQProperties rocketMQProperties;

    @Override
    public void run(String... args) {
        // 发送普通消息
        this.sendNormalForCommon();
        this.sendNormalForOrderVO();
        this.asyncSendDelayMessage();
    }

    private void sendNormalForCommon() {
        // 普通消息 + 自定义属性
        String topic = rocketMQProperties.getProducer().getDefaultNormalTopic();
        Object body = "mask coming.";
        String tag = "tag-common";
        Map<String, String> properties = Maps.of("appName", "spring-boot-rocketmq5-demo-producer");
        SendResult sendResult = rocketMQTemplate.send(tag, body, properties);
        log.info("Send order message success topic = {} ,key = {} ,body = {} ,sendResult = {}", topic, null, body, sendResult);
    }

    private void sendNormalForOrderVO() {
        Long id = RandomUtil.nextLong();
        OrderVO orderVO = new OrderVO(id, LocalDateTimes.now(), BigDecimal.valueOf(RandomUtil.nextDouble()), "同步的的随便了");
        String topic = rocketMQProperties.getProducer().getDefaultNormalTopic();
        String tag = "tag-order";
        SendResult sendResult = rocketMQTemplate.send(tag, orderVO, String.valueOf(id));
        log.info("Send order message success topic = {} ,key = {} ,body = {} ,sendResult = {}", topic, id, orderVO, sendResult);
    }

    private void asyncSendDelayMessage() {
        // 普通的消息
        OrderVO orderVO = new OrderVO(RandomUtil.nextLong(), LocalDateTimes.now(), BigDecimal.valueOf(RandomUtil.nextDouble()), "异步的，搞搞了");
        String topic = rocketMQProperties.getProducer().getDefaultNormalTopic();
        String tag = "tag-delay";
        // 30s后消费
        rocketMQTemplate.asyncSendDelaySeconds(tag, orderVO, 30, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                log.info("Async send normal message[{}] to topic[{}] result is {}", orderVO, topic, sendResult);
            }

            @Override
            public void onException(Throwable e) {
                log.error("Async send normal message[{}] to topic[{}] result is fail", orderVO, topic, e);
            }
        });
    }


}
