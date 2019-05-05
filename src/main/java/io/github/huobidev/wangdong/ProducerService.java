package io.github.huobidev.wangdong;

import io.github.huobidev.Order;
import java.util.Random;
import javax.annotation.Resource;
import org.springframework.stereotype.Service;

@Service
public class ProducerService {

    @Resource
    private Producer producer;

    public void send() {
        String[] symbols = {"btcusdt", "ethusdt", "bccusdt"};
        Double[] prices = {100d, 10.23d, 12.34d, 23.34d};

        int gen = 1000;
        Order order = new Order();
        Random random = new Random();
        for (int i = 0; i < gen; i++) {
            long time = System.currentTimeMillis();
            order.setId(time);
            order.setTs(time);
            order.setSymbol(symbols[random.nextInt(symbols.length)]);
            order.setPrice(prices[random.nextInt(prices.length)]);

            producer.send(order);
        }
    }

    public boolean send(Order order) {
        return producer.send(order);
    }
}
