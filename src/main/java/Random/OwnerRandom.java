package Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import scala.Char;

import java.util.Properties;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class OwnerRandom {
    private final static String TOPIC = "reorder-topic";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) throws InterruptedException {
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

        while (true) {
            Random rand = new Random(System.currentTimeMillis());
            char c = (char) (Math.abs(rand.nextInt() % 25) + 65);
            String item = "prod " + c;

            int quantity = Math.abs(rand.nextInt() % 50) +1;

            String msg = item + '$' + quantity;
            try {
                System.out.println("[Producer] " + msg);
                producer.send(new ProducerRecord<>("reorder-topic", msg, msg));
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }

            TimeUnit.SECONDS.sleep(10);

        }
    }
}