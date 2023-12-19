package cn.tmkit.mq.rocketmq5.boot.serializer;

import cn.tmkit.json.sjf4j.jackson.JacksonUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;

/**
 * 基于{@code Jackson 2}的消息序列化器
 *
 * @author ming.tang
 * @version 0.0.1
 * @date 2023-12-18
 */
public class GenericJackson2JsonRocketMQMessageSerializer implements RocketMQMessageSerializer<Object> {

    private final ObjectMapper objectMapper;

    /**
     * 创建{@linkplain GenericJackson2JsonRocketMQMessageSerializer}，使用自定义的{@linkplain ObjectMapper}
     *
     * @param objectMapper {@code Jackson 2}的对象映射器
     */
    public GenericJackson2JsonRocketMQMessageSerializer(@NotNull ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /**
     * 创建默认的{@linkplain GenericJackson2JsonRocketMQMessageSerializer}，使用{@linkplain JacksonUtil#createObjectMapperWithClass()}
     */
    public GenericJackson2JsonRocketMQMessageSerializer() {
        this(JacksonUtil.createObjectMapperWithClass());
    }

    /**
     * 将给定的对象序列为二进制内容
     *
     * @param payload 给定的对象
     * @return 二进制内容
     */
    @Override
    public byte[] serialize(@NotNull Object payload) {
        try {
            return objectMapper.writeValueAsBytes(payload);
        } catch (JsonProcessingException e) {
            throw new SerializationException("Could not write JSON: " + e.getMessage(), e);
        }
    }

    /**
     * 将二进制内容反序列化为对象
     *
     * @param data 二进制内容
     * @return 反序列化后的对象
     */
    @Override
    public @NotNull Object deserialize(byte[] data) {
        try {
            return objectMapper.readValue(data, Object.class);
        } catch (IOException e) {
            throw new SerializationException("Could not read JSON: " + e.getMessage(), e);
        }
    }

}
