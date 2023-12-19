package cn.tmkit.mq.rocketmq5.boot.core;

import cn.tmkit.core.exception.GenericRuntimeException;

/**
 * 消除处理异常
 *
 * @author ming.tang
 * @version 0.0.1
 * @date 2023-12-18
 */
public class MessageException extends GenericRuntimeException {

    /**
     * Constructs a new {@link MessageException} instance.
     *
     * @param msg 错误的详细信息
     */
    public MessageException(String msg) {
        super(msg);
    }

    /**
     * Constructs a new {@link MessageException} instance.
     *
     * @param msg   错误的详细信息
     * @param cause 内部异常
     */
    public MessageException(String msg, Throwable cause) {
        super(msg, cause);
    }

}
