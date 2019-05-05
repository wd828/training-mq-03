package io.github.huobidev.wangdong;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.huobidev.Order;
import io.github.huobidev.wangdong.config.KafkaConfig;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class ConsumerService {

    private final static Logger LOGGER = LoggerFactory.getLogger(ConsumerService.class);

    private KafkaConsumer<String, byte[]> consumer;

    private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private volatile Boolean enable = true;

    public static ThreadPoolExecutor THREADPOOL_EXECUTOR =
        (ThreadPoolExecutor) Executors.newFixedThreadPool(16);

    @PostConstruct
    private void init() {
        consumer = KafkaConfig.consumer();
        THREADPOOL_EXECUTOR.execute(this::handle);
    }

    private void handle() {
        while (enable) {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(100L));
            records.forEach(o -> {
                try {

                    Order order = OBJECT_MAPPER.readValue(o.value(), Order.class);
                    // 去重
                    LOGGER.info("[consumer]====> {}",OBJECT_MAPPER.writeValueAsString(order));
                } catch (IOException e) {
                    LOGGER.error("[消费] 转换失败!", e);
                }
            });
            consumer.commitSync();
        }
        consumer.close();
    }

    @PreDestroy
    public void desotry() {
        enable = false;
    }
}
