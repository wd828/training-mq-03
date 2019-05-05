package io.github.huobidev.wangdong;

import io.github.huobidev.BaseTest;
import io.github.huobidev.Order;
import javax.annotation.Resource;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class ProducerServiceTest extends BaseTest {

    @Resource
    private ProducerService producerService;

    @Test
    public void send() {
        Order order = new Order();
        Long ts = System.nanoTime();
        order.setPrice(123.456d);
        order.setSymbol("btcusdt");
        order.setTs(ts);
        order.setId(ts);

        boolean res = producerService.send(order);
        assertTrue(res);
    }
}
