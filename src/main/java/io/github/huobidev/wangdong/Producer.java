package io.github.huobidev.wangdong;

import io.github.huobidev.Order;

public interface Producer {

    /**
     * 发送订单消息
     *
     * @param order 订单消息
     */
    boolean send(Order order);
}
