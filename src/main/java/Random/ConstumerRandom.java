package Random;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.sql.*;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class ConstumerRandom {
    private static Connection connect() {
        // SQLite connection string
        String url = "jdbc:sqlite:kafka-linhares-db.db";
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return conn;
    }

    private static Properties ConsumerProps(String pid) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", pid);
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

    public static void main(String[] args) throws InterruptedException {
        String pid = String.valueOf(ProcessHandle.current().pid());
        System.out.println("Costumer running... " + pid);
        Properties props_producer = ProducerProps();
        Producer<String, String> producer = new KafkaProducer<>(props_producer);
        Properties props_consumer = ConsumerProps(pid);
        Consumer<String, String> consumer = new KafkaConsumer<String, String>(props_consumer);
        while (true){

            try (Connection conn = connect();
                 PreparedStatement ps = conn.prepareStatement("SELECT * FROM items WHERE id = (abs(random()) % (select (select max(id) from items)+1)) and stock > 0")
            ) {
                ResultSet rs;
                rs = ps.executeQuery();

                if (rs.next()) {
                    Random rand = new Random(System.currentTimeMillis());
                    int rand_q = Math.abs(rand.nextInt() % rs.getInt(4)) +1;
                    String msg = rs.getString(1)+ '$' + rs.getString(3) + '$' + String.valueOf(rand_q) + '$' + pid;
                    System.out.println("[Producer] " + msg);
                    try {
                        producer.send(new ProducerRecord<>("purchases-topic", "purchases", msg));
                    } catch (Exception e) {
                        System.out.println(e.getMessage());
                    }
                }

            } catch (SQLException e) {
                System.out.println(e.getMessage());

            }

            String topic_aux = "my-reply-topic-" + pid;
            consumer.subscribe(Arrays.asList(topic_aux));

            boolean ver = true;
            while (ver){
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("\n[Consumer] " + record.value());
                    System.out.println();
                    ver = false;
                    break;
                }
            }

            TimeUnit.SECONDS.sleep(5);
        }
    }
}