package cn.tmkit.mq.rocketmq5.boot.serializer;

import cn.tmkit.core.exception.GenericRuntimeException;

/**
 * 序列化、反序列化异常
 *
 * @author ming.tang
 * @version 0.0.1
 * @date 2023-12-18
 */
public class SerializationException extends GenericRuntimeException {

    /**
     * Constructs a new {@link SerializationException} instance.
     *
     * @param msg 错误的详细信息
     */
    public SerializationException(String msg) {
        super(msg);
    }

    /**
     * Constructs a new {@link SerializationException} instance.
     *
     * @param msg   错误的详细信息
     * @param cause 内部异常
     */
    public SerializationException(String msg, Throwable cause) {
        super(msg, cause);
    }

}
