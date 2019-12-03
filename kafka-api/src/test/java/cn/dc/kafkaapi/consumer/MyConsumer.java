package cn.dc.kafkaapi.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;

public class MyConsumer {
    @Test
    void m1() {
        /*
        匿名内部类的方式初始化，里面的括号表示创建一个内部类
        主题不在创建consumer时配置，要创建完之后调用subscribe方法配置
         */
        Properties props = new Properties() {{
            put("bootstrap.servers", "hadoop102:9092");
            put("group.id", "test");//CG
            put("enable.auto.commit", "true");//自动提交
            put("auto.commit.interval.ms", "1000");
            put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");//当换组或者topic中数据被删除时，offset如何设置，默认latest
        }};
        /*
        consumer也需要关闭
         */
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);){
            /*
            参数是个集合，可以订阅多个
             */
            consumer.subscribe(Stream.of("first").collect(toSet()));
             /*
            长轮循中一次轮循如果没获取到数据时的等待时间
             */
            ConsumerRecords<String, String> records = consumer.poll(100);
            /*
            死循环必须
             */
            while (true) {
                for (ConsumerRecord<String, String> record : records)
                    /*
                    printf，不是print和println
                     */
                    System.out.printf("offset = %d, key = %s, value= %s%n", record.offset(), record.key(), record.value());
                //同步提交，当前线程会阻塞直到 offset 提交成功
                consumer.commitSync();
                //异步提交
                consumer.commitAsync((offsets, exception) -> {
                    if (exception != null) {
                        System.err.println("Commit failed for" + offsets);
                    }
                });
            }

        }
    }
}

