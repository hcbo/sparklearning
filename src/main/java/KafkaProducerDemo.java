import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
/**
 * 模拟spark streaming 的数据源
 */
public class KafkaProducerDemo {

    public static void main(String[] args) throws InterruptedException {

        Properties props = new Properties();

        String remoteKafka = "219.216.65.160:9092";
        String localKafka = "192.168.225.6:9092";

        //broker地址
        props.put("bootstrap.servers", remoteKafka);

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
        int no = 0;
        int wordIndex;


        while (true) {
            wordIndex = (int) (Math.random() * sources.length);
            no++;
            ProducerRecord record = new ProducerRecord("mfs1.0.1Test",String.valueOf(no) ,sources[wordIndex]);
            System.out.println(record.key()+"->"+record.value());
            producer.send(record);
            Thread.sleep(50);
        }


    }
}


