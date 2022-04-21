import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {
    public static void main(String[] arg) {
        Logger loger = LoggerFactory.getLogger(ConsumerDemo.class.getName());
        String bootstrap_server = "127.0.0.1:9092";
        String group_id="My_sec_group_id";
        String topic="TestTopics";
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_server);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,group_id);
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");

        //CREATE  Consumer topic.
        KafkaConsumer<String,String> consumer= new KafkaConsumer<String, String>(props);
        consumer.subscribe(Collections.singletonList(topic));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("Value :: "+record.value());
                    loger.info("key :: " + record.key() + " " + "Value :: " + record.value());
                    loger.info("Topic :: " + record.topic() + " " + "Partition :: " + record.partition());
                    loger.info("Offset :: " + record.offset() + " " + "Headers :: " + record.headers());
                }
            }
        }catch (Exception e){
           System.out.println("While Statement Exception :: "+e.fillInStackTrace());
        }
    }
}