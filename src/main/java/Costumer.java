import java.sql.*;
import java.util.Arrays;
import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Costumer {

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

    public static void menu(){
        try (Connection conn = connect();
             PreparedStatement ps = conn.prepareStatement("SELECT * FROM items")
        ) {
            ResultSet rs;
            rs = ps.executeQuery();
            System.out.println("Items List [id]:");
            while (rs.next()) {
                System.out.println("\t[" + rs.getString(1) + "] " + rs.getString(2) );// + " " + rs.getString(3) + " " + rs.getString(4) + " " + rs.getString(5));
            }
            System.out.println("\t[-1] Quit\n");

        } catch (SQLException e) {
            System.out.println(e.getMessage());

        }

    }

    public static void main(String[] args) {
        String pid = String.valueOf(ProcessHandle.current().pid());
        System.out.println("Costumer running... " + pid);
        Properties props_producer = ProducerProps();
        Producer<String, String> producer = new KafkaProducer<>(props_producer);
        Properties props_consumer = ConsumerProps(pid);
        Consumer<String, String> consumer = new KafkaConsumer<String, String>(props_consumer);
        while (true){
            menu();
            Scanner input = new Scanner(System.in);
            System.out.print("Item select (by id)$ ");
            int item_id = input.nextInt();
            if (item_id == -1)
                break;
            System.out.print("Item price$ ");
            float item_price = input.nextFloat();
            System.out.print("Item quantity$ ");
            int item_quanty = input.nextInt();

            String msg = String.valueOf(item_id) + '$' + String.valueOf(item_price) + '$' + String.valueOf(item_quanty) + '$' + pid;
            try {
                producer.send(new ProducerRecord<>("purchases-topic", "purchases", msg));
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
            String topic_aux = "my-reply-topic-" + pid;
            System.out.println(topic_aux);
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
        }
        producer.close();
        consumer.close();
    }
}
