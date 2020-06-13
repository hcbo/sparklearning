import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class KafkaProducerTest {
    private static Log logger = LogFactory.getLog(KafkaProducerTest.class);
    private static final String broker = "kafka:9092";   //配置的是kafka broker的ip
    public static Producer<String, String> producer;
    public static AdminClient client;

    //用于开始的属性初始化
    public static void beforeClass() {
        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, broker);
        client = AdminClient.create(properties);
    }
    //用于关闭client
    public  static  void afterClass(){
        client.close();
    }

    //调用NewTopic  API的demo
    public static void createTopics(String topic) {
        try {
            NewTopic newTopic = new NewTopic(topic, 3, (short) 1);
            Collection<NewTopic> newTopics = new ArrayList<>();
            newTopics.add(newTopic);
            System.out.println(newTopics);
            // 创建主题
            CreateTopicsResult result=client.createTopics(newTopics);
            result.all().get();
            System.out.println("Topic创建成功！");
        } catch (InterruptedException e) {
            System.out.println("创建失败：" + e.getMessage());
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    //使用kafka的alterReplicaLogDirs修改replica的dir
    public  static  void  alterReplicaLogDir(String topic){
        try {
            TopicPartitionReplica topicPartitionReplica=new TopicPartitionReplica(topic,3,0);  //第二个参数是你想要移动的partition编号（编号从0开始），第三个参数是kafka集群上对应的broker 编号，此处是broker 0
            Map<TopicPartitionReplica, String> replicaAssignment=new HashMap<TopicPartitionReplica, String>();
            replicaAssignment.put(topicPartitionReplica,"/home/yhl/test/kafkalogs/ssd");  //此处的file path必须是kafka集群 broker 0 上server.properties 中 log.dirs配置中的一个
            System.out.println("开始移动log文件");
            AlterReplicaLogDirsResult  result=client.alterReplicaLogDirs(replicaAssignment);
            result.all().get();
            System.out.println("Topic移动文件夹成功！");
        }catch (InterruptedException e) {
            System.out.println("移动文件失败：" + e.getMessage());
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }


    //producer初始化
    static synchronized void initProducer() {
        if (producer != null) {
            producer.close();
            producer = null;
        }
        Properties props = new Properties();
        props.put("bootstrap.servers", broker);
        props.put("acks", "all");
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //定义生产者对象，该类指定了两个参数的泛型，第一个参数表示分区键值的类型，第二参数表示消息类型
        producer = new KafkaProducer<String, String>(props);
    }

    //批量发送数据
    public void testBachProfucuer(String topic) {
        try {
            for (int i = 0; i < 10000; i++) {
                producer.send(new ProducerRecord<String, String>(topic, Integer.toString(i), Integer.toString(i)));
            }
        } catch (ProducerFencedException e) {
            producer.close();
        }
        producer.close();
    }

    public static void main(String[] args) {
        beforeClass();   //必须初始化properties，否则不能正常创建topic，会提示空指针异常
        createTopics("test");
        initProducer();
        System.out.println("开始发送"+System.currentTimeMillis());
        new KafkaProducerTest().testBachProfucuer("test");
        System.out.println("发送完成"+System.currentTimeMillis());
        alterReplicaLogDir("test");
        afterClass();
    }
}
