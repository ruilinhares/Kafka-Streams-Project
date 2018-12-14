package REST;

import java.util.List;
import java.util.Scanner;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;


public class MyRESTClient {
    public static void main(String[] args) {
        Client client = ClientBuilder.newClient();
        WebTarget webTarget = client.target("http://localhost:9998/items");
        Invocation.Builder invocationBuilder =  webTarget.request(MediaType.APPLICATION_JSON);

        webTarget.request().post(Entity.entity("kafka", MediaType.TEXT_PLAIN));

        while (true){
            System.out.println();
            System.out.println("\t[1] Number of items ever sold.");
            System.out.println("\t[2] Number of units sold of each item.");
            System.out.println("\t[3] Maximum price of each item sold so far.");
            System.out.println("\t[4] Average number of purchases per order of supplies.");
            System.out.println("\t[5] Revenue, expenses, and profit of the shop so far.");
            System.out.println("\t[6] 2.Number of units sold of each item by 2 minutes.");
            System.out.println("\t[7] 3.Maximum price of each item sold so far by 2 minutes.");
            System.out.println("\t[8] Item providing the highest profit over the last 2 minutes.");
            System.out.println("\t[-1] quit");
            Scanner scann = new Scanner(System.in);
            System.out.print("\nSelect option$ ");
            String input = scann.nextLine();

            switch (input){

                case "1":
                    webTarget = client.target("http://localhost:9998/items/itemsEverSold");
                    invocationBuilder =  webTarget.request(MediaType.APPLICATION_JSON);
                    List<String> response = invocationBuilder.get(List.class);
                    //response.forEach(System.out::println);
                    break;

                case "2":
                    webTarget = client.target("http://localhost:9998/items/ItemsEverSoldByItem");
                    invocationBuilder =  webTarget.request(MediaType.APPLICATION_JSON);
                    response = invocationBuilder.get(List.class);
                    break;

                case "3":
                    webTarget = client.target("http://localhost:9998/items/MaxPriceItem");
                    invocationBuilder =  webTarget.request(MediaType.APPLICATION_JSON);
                    response = invocationBuilder.get(List.class);
                    break;

                case "4":
                    webTarget = client.target("http://localhost:9998/items/AverageNumItemsSupplies");
                    invocationBuilder =  webTarget.request(MediaType.APPLICATION_JSON);
                    response = invocationBuilder.get(List.class);
                    break;

                case "5":
                    webTarget = client.target("http://localhost:9998/items/Revenue");
                    invocationBuilder =  webTarget.request(MediaType.APPLICATION_JSON);
                    response = invocationBuilder.get(List.class);
                    break;


                case "6":
                    webTarget = client.target("http://localhost:9998/items/ItemsEverSoldByItemByXmin");
                    invocationBuilder =  webTarget.request(MediaType.APPLICATION_JSON);
                    response = invocationBuilder.get(List.class);
                    break;


                case "7":
                    webTarget = client.target("http://localhost:9998/items/MaxPriceItemByXmin");
                    invocationBuilder =  webTarget.request(MediaType.APPLICATION_JSON);
                    response = invocationBuilder.get(List.class);
                    break;


                case "8":
                    webTarget = client.target("http://localhost:9998/items/ItemProvidingHighestProfit");
                    invocationBuilder =  webTarget.request(MediaType.APPLICATION_JSON);
                    response = invocationBuilder.get(List.class);
                    break;

                case "-1":
                    System.out.println("\nbye...");
                    return;

                default:
                    System.out.println("\t*invalid input*");
                    break;
            }
        }
    }
}