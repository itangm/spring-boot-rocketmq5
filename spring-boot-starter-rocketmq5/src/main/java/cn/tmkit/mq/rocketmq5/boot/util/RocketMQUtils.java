package cn.tmkit.mq.rocketmq5.boot.util;

import cn.tmkit.core.lang.Strings;
import cn.tmkit.core.lang.reflect.Singletons;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientConfigurationBuilder;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.StaticSessionCredentialsProvider;

import java.time.Duration;
import java.util.Objects;

/**
 * 工具类
 *
 * @author ming.tang
 * @version 0.0.1
 * @date 2023-12-18
 */
public class RocketMQUtils {

    /**
     * 获取 ClientServiceProvider
     *
     * @return {@linkplain ClientServiceProvider}
     */
    public static ClientServiceProvider getClientServiceProvider() {
        return Singletons.get(ClientServiceProvider.class.getName(), ClientServiceProvider::loadService);
    }

    /**
     * 创建 ClientConfiguration
     *
     * @param endpoints      端点
     * @param enableSsl      是否启用ssl
     * @param accessKey      用户名
     * @param secretKey      密钥
     * @param requestTimeout 请求超时时间
     * @return {@linkplain ClientConfiguration}
     */
    public static ClientConfiguration createClientConfiguration(String endpoints, boolean enableSsl, String accessKey,
                                                                String secretKey, Duration requestTimeout) {

        ClientConfigurationBuilder clientConfigurationBuilder = ClientConfiguration.newBuilder()
                .enableSsl(enableSsl)
                .setEndpoints(endpoints);
        if (Strings.isAllNotBlank(accessKey, secretKey)) {
            clientConfigurationBuilder.setCredentialProvider(new StaticSessionCredentialsProvider(accessKey, secretKey));
        }
        if (Objects.nonNull(requestTimeout)) {
            clientConfigurationBuilder.setRequestTimeout(requestTimeout);
        }
        return clientConfigurationBuilder.build();
    }

}
