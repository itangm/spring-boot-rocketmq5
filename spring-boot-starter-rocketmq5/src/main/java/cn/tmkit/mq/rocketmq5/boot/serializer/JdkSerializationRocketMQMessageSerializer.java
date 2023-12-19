package cn.tmkit.mq.rocketmq5.boot.serializer;

import cn.tmkit.core.lang.Asserts;
import org.jetbrains.annotations.NotNull;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.serializer.support.DeserializingConverter;
import org.springframework.core.serializer.support.SerializingConverter;

/**
 * @author ming.tang
 * @version 0.0.1
 * @date 2023-12-18
 */
@SuppressWarnings("ConstantConditions")
public class JdkSerializationRocketMQMessageSerializer implements RocketMQMessageSerializer<Object> {

    private final Converter<Object, byte[]> serializer;
    private final Converter<byte[], Object> deserializer;

    public JdkSerializationRocketMQMessageSerializer() {
        this(new SerializingConverter(), new DeserializingConverter());
    }

    public JdkSerializationRocketMQMessageSerializer(Converter<Object, byte[]> serializer, Converter<byte[], Object> deserializer) {
        Asserts.notNull(serializer, "Serializer must not be null!");
        Asserts.notNull(deserializer, "Deserializer must not be null!");
        this.serializer = serializer;
        this.deserializer = deserializer;
    }

    /**
     * 将给定的对象序列为二进制内容
     *
     * @param payload 给定的对象
     * @return 二进制内容
     */
    @Override
    public byte[] serialize(@NotNull Object payload) {
        return serializer.convert(payload);
    }

    /**
     * 将二进制内容反序列化为对象
     *
     * @param data 二进制内容
     * @return 反序列化后的对象
     */
    @Override
    public @NotNull Object deserialize(byte[] data) {
        return Asserts.notNull(deserializer.convert(data));
    }

}
