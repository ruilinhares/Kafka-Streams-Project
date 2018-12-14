import java.util.Arrays;
import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Supplier {

    private static Properties ConsumerProps() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "suplliers");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

    private static Properties ProducerProps() {
        // create instance for properties to access producer configs
        Properties props = new Properties();

        //Assign localhost id
        props.put("bootstrap.servers", "localhost:9092");

        //Set acknowledgements for producer requests.
        props.put("acks", "all");

        //If the request fails, the producer can automatically retry,
        props.put("retries", 0);

        //Specify buffer size in config
        props.put("batch.size", 16384);

        //Reduce the no of requests less than 0
        props.put("linger.ms", 1);

        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put("buffer.memory", 33554432);

        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");


        return props;
    }

    public static void main(String[] args) {
        System.out.println("Shimpents running...");
        Properties props_producer = ProducerProps();
        Producer<String, String> producer = new KafkaProducer<>(props_producer);
        Properties props_consumer = ConsumerProps();
        Consumer<String, String> consumer = new KafkaConsumer<String, String>(props_consumer);

        consumer.subscribe(Arrays.asList("reorder-topic"));

        while (true){
            ConsumerRecords<String, String> records = consumer.poll(100);

            for (ConsumerRecord<String, String> record : records) {
                System.out.println("[Consumer] Item received: " + record.value());
                Scanner input = new Scanner(System.in);
                System.out.print("Item price$ ");
                float price = input.nextFloat();

                String msg = record.value() + '$' + price;
                try {
                    producer.send(new ProducerRecord<>("shipments-topic", msg, msg));
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                }

            }
        }
    }
}
