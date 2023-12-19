package cn.tmkit.mq.rocketmq5.boot.demo.vo;

import lombok.*;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * 订单信息
 *
 * @author ming.tang
 * @version 0.0.1
 * @date 2023-12-18
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class OrderVO {

    /**
     * 订单id
     */
    private Long id;

    /**
     * 订单日期
     */
    private LocalDateTime orderDate;

    /**
     * 订单金额
     */
    private BigDecimal orderAmt;

    /**
     * 订单描述
     */
    private String orderDesc;

}
