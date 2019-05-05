package io.github.huobidev.wangdong.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.huobidev.Order;
import io.github.huobidev.wangdong.Producer;
import io.github.huobidev.wangdong.config.KafkaConfig;
import java.util.concurrent.Future;
import javax.annotation.PostConstruct;
import kafka.common.KafkaException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class ProducerImpl implements Producer {

    private final Logger LOGGER = LoggerFactory.getLogger(ProducerImpl.class);

    private KafkaProducer kafkaProducer;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
        .configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);

    @PostConstruct
    private void init() {
        kafkaProducer = KafkaConfig.producer();
        kafkaProducer.initTransactions();
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean send(Order order) {
        try {
            byte[] value = OBJECT_MAPPER.writeValueAsBytes(order);
            LOGGER.info("Send value: {}", value);
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(KafkaConfig.getTopic(),
                String.valueOf(order.getId()), value);
            kafkaProducer.beginTransaction();
            Future<RecordMetadata> future = kafkaProducer.send(record);
            kafkaProducer.flush();
            kafkaProducer.commitTransaction();
            if (future.isDone()) {
                return true;
            }
        } catch (JsonProcessingException e) {
            LOGGER.error("[发送] 序列化失败, Order Id: {}", order.getId(), e);
        } catch (KafkaException e) {
            LOGGER.error("[发送] 发送失败", e);
            kafkaProducer.abortTransaction();
        }
        kafkaProducer.close();
        return false;
    }
}
