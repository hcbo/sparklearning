import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.UUID;

/**
 * 模拟spark streaming 的数据源
 */
public class KafkaProducerDemo {

    public static void main(String[] args) throws InterruptedException {

        Properties props = new Properties();

        //broker地址
        props.put("bootstrap.servers", "192.168.225.6:9092");

        //请求时候需要验证ide
        props.put("acks", "-1");

        //请求失败时候需要重试
        props.put("retries", 3);

        //内存缓存区大小
        props.put("buffer.memory", 33554432);

        //指定消息key序列化方式
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        //指定消息本身的序列化方式
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(props);
        String[] sources = {"spark", "hadoop", "flink", "hbase", "kafka"};
        int wordIndex;


        while (true) {
            wordIndex = (int) (Math.random() * sources.length);

            ProducerRecord record = new ProducerRecord("for_spark", UUID.randomUUID().toString(),sources[wordIndex]);
            System.out.println(record.key()+"->"+record.value());
            producer.send(record);
            Thread.sleep(1000);
        }


    }
}


