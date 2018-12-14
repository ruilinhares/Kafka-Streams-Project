
import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Owner {
    private final static String TOPIC = "reorder-topic";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) {
        Properties props = new Properties();
        //Assign localhost id
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);

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
        Producer<String, String> producer = new KafkaProducer<>(props);

        while (true){
            System.out.print("\nNew item$ ");
            Scanner input = new Scanner(System.in);
            String item = input.nextLine();
            if (item.equals("quit()")){
                break;
            }
            System.out.print("\nQuantity$ ");
            int quantity = input.nextInt();

            String msg = item + '$' + quantity;
            try {
                producer.send(new ProducerRecord<>("reorder-topic", msg, msg));
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }


        }
        producer.close();
    }
}
