package io.github.huobidev.wangdong.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaConfig {

    private final static String SERVERS = "127.0.0.1:9092";

    private final static String TOPIC = "training-mq-03";

    public static String getTopic() {
        return TOPIC;
    }

    public static KafkaConsumer<String, byte[]> consumer() {
        Properties pro = new Properties();
        pro.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
        pro.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        pro.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        pro.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "training-mq-wd");
        // 从最早一条开始消费
        pro.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // 禁止自动提交
        pro.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(pro);
        consumer.subscribe(Collections.singletonList(TOPIC));
        return consumer;
    }

    public static KafkaProducer producer() {
        Properties pro = new Properties();
        pro.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
        pro.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        pro.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        pro.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(10 * 1024));
        pro.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
        pro.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        pro.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, String.valueOf(32 * 1024 * 1024));
        // 避免消息乱序(限制客户端在单个连接上能够发送的未响应请求的个数)
        pro.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
        // 消息可靠性
        pro.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        pro.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        pro.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, TOPIC + "_transaction_id");
        return new KafkaProducer(pro);
    }

}
