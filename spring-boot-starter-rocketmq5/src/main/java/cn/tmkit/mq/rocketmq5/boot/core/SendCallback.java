package cn.tmkit.mq.rocketmq5.boot.core;

/**
 * 发送结果的回调
 *
 * @author ming.tang
 * @version 0.0.1
 * @date 2023-12-18
 */
public interface SendCallback {

    /**
     * 发送成功
     *
     * @param sendResult 结果
     */
    void onSuccess(final SendResult sendResult);

    /**
     * 发送失败
     *
     * @param e 异常
     */
    void onException(final Throwable e);

}
