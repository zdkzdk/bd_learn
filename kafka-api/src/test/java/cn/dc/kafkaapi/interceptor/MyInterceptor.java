package cn.dc.kafkaapi.interceptor;

import org.apache.kafka.clients.producer.*;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * （1）configure(configs)
 * 获取配置信息和初始化数据时调用。
 * （2）onSend(ProducerRecord)：
 * 该方法封装进 KafkaProducer.send 方法中，即它运行在用户主线程中。Producer 确保在
 * 消息被序列化以及计算分区前调用该方法。用户可以在该方法中对消息做任何操作，但最好
 * 保证不要修改消息所属的 topic 和分区，否则会影响目标分区的计算。
 * （3）onAcknowledgement(RecordMetadata, Exception)：
 * 该方法会在消息从 RecordAccumulator 成功发送到 Kafka Broker 之后，或者在发送过程
 * 中失败时调用。并且通常都是在 producer 回调逻辑触发之前。onAcknowledgement 运行在
 * producer 的 IO 线程中，因此不要在该方法中放入很重的逻辑，否则会拖慢 producer 的消息
 * 发送效率。
 * （4）close：
 * 关闭 interceptor，主要用于执行一些资源清理工作
 * 如前所述，interceptor 可能被运行在多个线程中，因此在具体实现时用户需要自行确保
 * 线程安全。另外倘若指定了多个 interceptor，则 producer 将按照指定顺序调用它们，并仅仅
 * 是捕获每个 interceptor 可能抛出的异常记录到错误日志中而非在向上传递。这在使用过程中
 * 要特别留意。
 * <p>
 * 需求：
 * 实现一个双 interceptor 组成的拦截链。
 * 第一个 interceptor 会在消息发送前将时间戳信息加到消息 value 的最前部；
 * 第二个 interceptor 会在消息发送后更新成功发送消息数或失败发送消息数。
 */
public class MyInterceptor {
    @Test
    void testInterceptor() {
        // 1 设置配置信息
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop102:9092");
        props.put("acks", "all");
        props.put("retries", 3);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 2 构建拦截链
        List<String> interceptors = new ArrayList<>();
        interceptors.add("com.atguigu.kafka.interceptor.TimeInterceptor");
        interceptors.add("com.atguigu.kafka.interceptor.CounterInterceptor");
        // 3设置interceptors
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);
        String topic = "first";
        Producer<String, String> producer = new KafkaProducer<>(props);
        // 4 发送消息
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, "message" + i);
            producer.send(record);
        }
        // 5 一定要关闭 producer，这样才会调用 interceptor 的 close 方法
        producer.close();
    }
}

/**
 * 给record添加事件标记
 */
class TimeInterceptor implements ProducerInterceptor<String, String> {
    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        // 创建一个新的 record，把时间戳写入消息体的最前部
        return new ProducerRecord(record.topic(), record.partition(), record.timestamp(), record.key(),
                System.currentTimeMillis() + "," + record.value().toString());
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
    }

    @Override
    public void close() {
    }
}

/**
 * 计数
 */
class CounterInterceptor implements ProducerInterceptor<String, String> {
    private int errorCounter = 0;
    private int successCounter = 0;

    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        // 统计成功和失败的次数
        if (exception == null) {
            successCounter++;
        } else {
            errorCounter++;
        }
    }

    /*
    打印结果
     */
    @Override
    public void close() {

        System.out.println("Successful sent: " + successCounter);
        System.out.println("Failed sent: " + errorCounter);
    }
}