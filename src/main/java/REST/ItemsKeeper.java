package REST;

import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.*;
import scala.None;

import java.util.*;

import java.util.concurrent.TimeUnit;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import java.util.regex.Pattern;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/items")
public class ItemsKeeper {
    //XXX: we have several threads...
    private static KafkaStreams streams;
    private static Properties props;
    private static String PURCHASES_TOPIC = "purchases-topic";
    private static String REORDER_TOPIC = "reorder-topic";
    private static String SHIPMENTS_TOPIC = "shipments-topic";
    private static String MY_REPLY_TOPIC = "my-reply-topic";
    private static String PROFIT_TOPIC = "profit-topic";
    private static String MAX_TOPIC = "max-topic";

    private static String PURCHASES_TABLE = "purchases-table";
    private static String ITEMS_SOLD_TABLE = "items-sold-table";


    @POST
    public void startStreams(String start) throws InterruptedException {

        streams = createStream();
        streams.start();
        Thread.sleep(1000);
        System.out.println("Streams have started");
    }

    private KafkaStreams createStream() {
        props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-stream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        //props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG,2);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> purchases = builder.stream(PURCHASES_TOPIC);

        KStream<String, String> my_reply = builder.stream(MY_REPLY_TOPIC);

        KStream<String,String> shipments = builder.stream(SHIPMENTS_TOPIC);

        KStream<String,String> reorder = builder.stream(REORDER_TOPIC);

        KStream<String,String> profit = builder.stream(PROFIT_TOPIC);

        KStream<String,String> max = builder.stream(MAX_TOPIC);

        KTable<String,Long> count_items_sold = my_reply.
                groupByKey().
                count(Materialized.as(MY_REPLY_TOPIC));
        count_items_sold.mapValues(v -> " " + v).toStream();

        KTable<String, Long> count_purchases = purchases.
                groupByKey().
                count(Materialized.as(PURCHASES_TOPIC));
        count_purchases.mapValues(v -> " " + v).toStream();

        KTable<String, Long> count_max = max.
                groupByKey().
                count(Materialized.as(MAX_TOPIC));
        count_max.mapValues(v -> " " + v).toStream();

        KTable<String, Long> count_reorders = reorder.
                groupByKey().
                count(Materialized.as(REORDER_TOPIC));
        count_reorders.mapValues(v -> " " + v).toStream();

        KTable<String, Long> count_profit = profit.
                groupByKey().
                count(Materialized.as(PROFIT_TOPIC));
        count_profit.mapValues(v -> " " + v).toStream();

        KTable<String, Long> count_shipments = shipments.
                groupByKey().
                count(Materialized.as(SHIPMENTS_TOPIC));
        count_shipments.mapValues(v -> " " + v).toStream();

        KTable<Windowed<String>, Long> x_min_replies = my_reply.groupByKey().
                windowedBy(TimeWindows.of(TimeUnit.MINUTES.toMillis(2))).
                count(Materialized.as(ITEMS_SOLD_TABLE));
        x_min_replies.toStream((wk, v) -> wk.key()).map((k, v) -> new KeyValue<>(k, "" + k + "-->" + v)).to("xmin");

        KTable<Windowed<String>, Long> max_purchases = max.groupByKey().
                windowedBy(TimeWindows.of(TimeUnit.MINUTES.toMillis(2))).
                count(Materialized.as(PURCHASES_TABLE));
        max_purchases.toStream((wk, v) -> wk.key()).map((k, v) -> new KeyValue<>(k, "" + k + "-->" + v)).to("imax");

        return new KafkaStreams(builder.build(), props);

    }

    @Path("itemsEverSold")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public void ItemsEverSold(){
        System.out.println("\n\t *[1] Number of items ever sold.*\n");
        try {
            System.out.println(streams.state());

            ReadOnlyKeyValueStore<String, Long> keyValueStore = streams.store(MY_REPLY_TOPIC, QueryableStoreTypes.keyValueStore());

            System.out.println();

            // Get the values for a range of keys available in this application instance
            KeyValueIterator<String, Long> all = keyValueStore.all();
            int count = 0;
            while (all.hasNext()) {
                KeyValue<String, Long> next = all.next();
                count += next.value;
            }
            System.out.println("Quantity >> " + count);
            all.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Path("ItemsEverSoldByItem")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public void ItemsEverSoldByItem(){
        System.out.println("\n\t *[3] Number of units sold of each item.*\n");
        try {
            System.out.println(streams.state());

            ReadOnlyKeyValueStore<String, Long> keyValueStore = streams.store(MY_REPLY_TOPIC, QueryableStoreTypes.keyValueStore());

            System.out.println();

            // Get the values for a range of keys available in this application instance
            KeyValueIterator<String, Long> all = keyValueStore.all();
            while (all.hasNext()) {
                KeyValue<String, Long> next = all.next();
                System.out.println("Item: " + next.key + " Quantity >> " + next.value);
            }
            all.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Path("ItemsEverSoldByItemByXmin")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public void ItemsEverSoldByItemByXmin(){
        System.out.println("\n\t *[6] Number of units sold of each item by 2 minutes.*\n");
        try {
            System.out.println(streams.state());

            ReadOnlyWindowStore<String, Long> keyValueStore = streams.store(ITEMS_SOLD_TABLE, QueryableStoreTypes.windowStore());

            System.out.println();

            // Get the values for a range of keys available in this application instance
            KeyValueIterator<Windowed<String>, Long> all = keyValueStore.all();
            while (all.hasNext()) {
                KeyValue<Windowed<String>, Long> next = all.next();
                System.out.println("Item: " + next.key.key() + " Quantity >> " + next.value);
            }
            all.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Path("AverageNumItemsSupplies")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public void AverageNumItemsSupplies(){
        System.out.println("\n\t *[4] Average number of purchases per order of supplies.*\n");
        try {
            System.out.println(streams.state());

            ReadOnlyKeyValueStore<String, Long> keyValueStore = streams.store(REORDER_TOPIC, QueryableStoreTypes.keyValueStore());

            System.out.println();

            // Get the values for a range of keys available in this application instance
            KeyValueIterator<String, Long> all = keyValueStore.all();
            HashMap<String, List<Integer>> orders = new HashMap<>();
            while (all.hasNext()) {
                KeyValue<String, Long> next = all.next();
                String[] lista = next.key.split("\\$");
                int num = (int) (Integer.parseInt(lista[1]) * next.value);

                if (!orders.containsKey(lista[0])){
                    List<Integer> listaux = new ArrayList<>();
                    listaux.add(num);
                    listaux.add(Math.toIntExact(next.value));

                    orders.put(lista[0], listaux);
                }
                else{
                    List<Integer> listvalue = orders.get(lista[0]);

                    listvalue.set(0, listvalue.get(0) + num);
                    listvalue.set(1, listvalue.get(1) + Math.toIntExact(next.value));
                    orders.put(lista[0], listvalue);

                }
            }
            for (String map : orders.keySet()){
                List<Integer> listvalue = orders.get(map);
                System.out.println("Item: " + map + " Average >> " + listvalue.get(0) / listvalue.get(1));
            }
            all.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Path("MaxPriceItem")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public void MaxPriceItem(){
        System.out.println("\n\t *[3] Maximum price of each item sold so far.*\n");
        try {
            System.out.println(streams.state());

            ReadOnlyKeyValueStore<String, Long> keyValueStore = streams.store(MAX_TOPIC, QueryableStoreTypes.keyValueStore());

            System.out.println();

            // Get the values for a range of keys available in this application instance
            KeyValueIterator<String, Long> all = keyValueStore.all();
            HashMap<String, Float> orders = new HashMap<>();

            while (all.hasNext()) {
                KeyValue<String, Long> next = all.next();

                String[] lista = next.key.split("\\$");

                if (!orders.containsKey(lista[0])){
                    orders.put(lista[0], Float.parseFloat(lista[1]));
                }
                else{
                    if (Integer.parseInt(lista[1]) > orders.get(lista[0]))
                        orders.put(lista[0], Float.parseFloat(lista[1]));
                }
            }
            for (String map : orders.keySet()){
                System.out.println("Item: " + map + " Max price >> " + orders.get(map));
            }
            all.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Path("MaxPriceItemByXmin")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public void MaxPriceItemByXmin(){
        System.out.println("\n\t *[7] Maximum price of each item sold so far by 2 minutes.*\n");
        try {
            System.out.println(streams.state());

            ReadOnlyWindowStore<String, Long> keyValueStore = streams.store(PURCHASES_TABLE, QueryableStoreTypes.windowStore());

            System.out.println();

            // Get the values for a range of keys available in this application instance
            KeyValueIterator<Windowed<String>, Long> all = keyValueStore.all();
            HashMap<String, Float> orders = new HashMap<>();

            while (all.hasNext()) {
                KeyValue<Windowed<String>, Long> next = all.next();
                String[] lista = next.key.key().split("\\$");
                if (!orders.containsKey(lista[0])){
                    orders.put(lista[0], Float.parseFloat(lista[1]));
                }
                else{
                    if (Float.parseFloat(lista[1]) > orders.get(lista[0]))
                        orders.put(lista[0], Float.parseFloat(lista[1]));
                }
            }
            for (String map : orders.keySet()){
                System.out.println("Item: " + map + " Max price >> " + orders.get(map));
            }
            all.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Path("Revenue")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public void Revenue(){
        System.out.println("\n\t *[5] Revenue, expenses, and profit of the shop so far.*\n");
        try {
            System.out.println(streams.state());

            ReadOnlyKeyValueStore<String, Long> keyValueStore_expenses = streams.store(SHIPMENTS_TOPIC, QueryableStoreTypes.keyValueStore());
            ReadOnlyKeyValueStore<String, Long> keyValueStore_profit = streams.store(PROFIT_TOPIC, QueryableStoreTypes.keyValueStore());

            System.out.println();

            // Get the values for a range of keys available in this application instance
            KeyValueIterator<String, Long> all_expenses = keyValueStore_expenses.all();
            KeyValueIterator<String, Long> all_profit = keyValueStore_profit.all();

            float count_expenses = 0;
            float count_profit = 0;

            while (all_expenses.hasNext()) {
                KeyValue<String, Long> next = all_expenses.next();
                String[] lista = next.key.split("\\$");
                count_expenses += (Integer.parseInt(lista[1]) * Float.parseFloat(lista[2])) * next.value;
            }

            while (all_profit.hasNext()) {
                KeyValue<String, Long> next = all_profit.next();
                float proaux = Float.parseFloat(next.key);
                count_profit += proaux * next.value ;
            }

            System.out.println("Revenue: " + String.format("%.2f",(count_profit - count_expenses) * 100 / count_expenses) + "%");
            System.out.println("Profit: " + count_profit);
            System.out.println("Expenses: " + count_expenses);

            all_expenses.close();
            all_profit.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Path("ItemProvidingHighestProfit")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public void ItemProvidingHighestProfit(){
        System.out.println("\n\t *[8] Item providing the highest profit over the last 2 minutes.*\n");
        try {
            System.out.println(streams.state());

            ReadOnlyWindowStore<String, Long> keyValueStore = streams.store(PURCHASES_TABLE, QueryableStoreTypes.windowStore());

            System.out.println();
            HashMap<String, Float> orders = new HashMap<>();

            // Get the values for a range of keys available in this application instance
            KeyValueIterator<Windowed<String>, Long> all = keyValueStore.all();
            while (all.hasNext()) {
                KeyValue<Windowed<String>, Long> next = all.next();

                String[] lista = next.key.key().split("\\$");
                if (!orders.containsKey(lista[0])){
                    orders.put(lista[0], Float.parseFloat(lista[1]) * next.value);
                }
                else{
                    if (Float.parseFloat(lista[1]) > orders.get(lista[0]))
                        orders.put(lista[0], Float.parseFloat(lista[1]) * next.value + orders.get(lista[0]));
                }
            }
            String item = null;
            float prof = -1;
            for (String map : orders.keySet()){
                System.out.println(map + " | " + orders.get(map));
                if (orders.get(map) > prof){
                    prof = orders.get(map);
                    item = map;
                }
            }
            System.out.println("Item: " + item + " Profit >> " + prof);
            all.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
