import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;


public class Shop {

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

    private static Properties ConsumerProps() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "shop");
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

    public static String Producer_anwser(String[] lista, Producer<String, String> producer){
        try (Connection conn = connect();
             PreparedStatement ps = conn.prepareStatement("SELECT * FROM items WHERE id=?");
             PreparedStatement ps_update = conn.prepareStatement("UPDATE items SET stock=? WHERE id=?")
        ) {
            ResultSet rs;
            ps.setInt(1, Integer.parseInt(lista[0]));
            rs = ps.executeQuery();
            if (rs.next()) {
                int id_item = rs.getInt(1);
                String item = rs.getString(2);
                float price = rs.getFloat(3);
                int stock = rs.getInt(4);
                int stock_initial = rs.getInt(5);
                double difer = stock - (Integer.parseInt(lista[2]));

                if (Math.abs(price - Float.parseFloat(lista[1])) > 0.1)
                    return "rejected$"+item;

                else if (stock - (Integer.parseInt(lista[2])) < 0){
                    int stock_aux = stock_initial - (Integer.parseInt(lista[2]) - stock);
                    String msg = item + "$" + String.valueOf(stock_aux);
                    producer.send(new ProducerRecord<>("reorder-topic", msg, msg));
                    System.out.println(msg + " | " + "reorder-topic");
                    return "wait";
                }
                else if ((difer / stock_initial) < 0.25){
                    System.out.println("stock baixo");
                    int stock_aux = stock - (Integer.parseInt(lista[2]));
                    ps_update.setInt(2, id_item);
                    ps_update.setInt(1, stock_aux);
                    ps_update.executeUpdate();

                    String msg = item + "$" + String.valueOf(stock_initial - stock_aux);
                    producer.send(new ProducerRecord<>("reorder-topic", msg, msg));
                    System.out.println(msg + " | " + "reorder-topic");

                    producer.send(new ProducerRecord<>("my-reply-topic", item, item));

                    String max_msg = item + "$" + String.valueOf(price);
                    producer.send(new ProducerRecord<>("max-topic", max_msg, max_msg));

                    float profit = price * Integer.parseInt(lista[2]);
                    producer.send(new ProducerRecord<>("profit-topic", String.valueOf(profit), item));

                    return "accept$"+item;
                }
                else{
                    ps_update.setInt(2, id_item);
                    ps_update.setInt(1, stock - (Integer.parseInt(lista[2])));
                    ps_update.executeUpdate();

                    producer.send(new ProducerRecord<>("my-reply-topic", item, item));

                    String max_msg = item + "$" + String.valueOf(price);
                    producer.send(new ProducerRecord<>("max-topic", max_msg, max_msg));

                    float profit = price * Integer.parseInt(lista[2]);
                    producer.send(new ProducerRecord<>("profit-topic", String.valueOf(profit), item));

                    return "accept$"+item;
                }
            }

        } catch (SQLException e) {
            System.out.println(e.getMessage());

        }
        return "none";
    }

    public static void main(String[] args) {
        System.out.println("Shop running...");
        Properties props_producer = ProducerProps();
        Producer<String, String> producer = new KafkaProducer<>(props_producer);
        Properties props_consumer = ConsumerProps();
        Consumer<String, String> consumer = new KafkaConsumer<String, String>(props_consumer);
        ArrayList<String[]> auxLista = new ArrayList<>();

        consumer.subscribe(Arrays.asList("purchases-topic","shipments-topic"));

        while (true){
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("[Pruchases consumer] " + record.value());
                if (record.topic().equals("purchases-topic")){
                    try {
                        String[] lista = record.value().split("\\$");

                        String msg = Producer_anwser(lista, producer);
                        if (msg.equals("wait")){
                            auxLista.add(lista);
                        }
                        else {
                            String topic_aux = "my-reply-topic-" + lista[3];
                            producer.send(new ProducerRecord<>(topic_aux, "1", msg));
                            System.out.println(msg + " | " + topic_aux);
                        }

                    } catch (Exception e) {
                        System.out.println(e.getMessage());
                    }
                }
                else if(record.topic().equals("shipments-topic")) {
                    System.out.println("[Shipments consumer] " + record.value());
                    String[] lista = record.value().split("\\$");

                    String item = lista[0];
                    int item_quanty = Integer.parseInt(lista[1]);
                    float item_price = (float) ((Float.parseFloat(lista[2])) * 1.30);


                    try (Connection conn = connect();
                         PreparedStatement ps = conn.prepareStatement("SELECT * FROM items WHERE prod=?");
                         PreparedStatement ps_update = conn.prepareStatement("UPDATE items SET stock=?, price=? WHERE id=?");
                         PreparedStatement ps_insert = conn.prepareStatement("INSERT INTO items(prod, price, stock, stock_initial) values  (?, ?, ?, ?)")
                    ) {

                        ps.setString(1, item);
                        ResultSet rs = ps.executeQuery();
                        if (rs.next()){
                            int item_id = rs.getInt(1);
                            int item_stock = rs.getInt(4);

                            ps_update.setInt(1,item_quanty + item_stock);
                            ps_update.setFloat(2,item_price);
                            ps_update.setInt(3,item_id);
                            ps_update.executeUpdate();


                            for (String[] aux : auxLista){
                                if (item_id == Integer.parseInt(aux[0])){
                                    if (item_stock >= Integer.parseInt(aux[2])){
                                        try {
                                            String topic_aux = "my-reply-topic-" + aux[3];
                                            String msg = "accepted$" + item + "$" + item_price;
                                            int stock = item_stock - Integer.parseInt(aux[2]);
                                            ps_update.setInt(1,stock);
                                            ps_update.setFloat(2,item_price);
                                            ps_update.setInt(3,item_id);
                                            ps_update.executeUpdate();

                                            producer.send(new ProducerRecord<>(topic_aux, "1", msg));

                                            producer.send(new ProducerRecord<>("my-reply-topic", item, item));

                                            float profit = item_price * Integer.parseInt(aux[2]);
                                            producer.send(new ProducerRecord<>("profit-topic", String.valueOf(profit), item));

                                            String max_msg = item + "$" + item_price;
                                            producer.send(new ProducerRecord<>("max-topic", max_msg, max_msg));

                                            System.out.println(msg + " | " + topic_aux);
                                            auxLista.remove(aux);

                                        } catch (Exception e) {
                                            System.out.println(e.getMessage());
                                        }
                                    }
                                }
                            }
                        }
                        else {
                            ps_insert.setString(1, item);
                            ps_insert.setFloat(2, item_price);
                            ps_insert.setInt(3, item_quanty);
                            ps_insert.setInt(4, item_quanty);
                            ps_insert.executeUpdate();
                        }

                    } catch (SQLException e) {
                        System.out.println(e.getMessage());

                    }
                }
            }
        }
    }
}
