package REST;

import java.net.URI;

import javax.ws.rs.core.UriBuilder;

import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

public class MyRESTServer {

    private final static int port = 9998;
    private final static String host="http://localhost/";

    public static void main(String[] args) {
        URI baseUri = UriBuilder.fromUri(host).port(port).build();
        ResourceConfig config = new ResourceConfig(ItemsKeeper.class);
        JdkHttpServerFactory.createHttpServer(baseUri, config);
    }
}
