import com.pelicanq.client.PelicanClient;
import com.pelicanq.client.types.*;

public class Main {
    public static void main(String[] args) throws Exception {
        try (PelicanClient client = PelicanClient.forAddress("127.0.0.1", 7072).build()) {
            boolean created = client.declareQueue("example-queue", new QueueOptions());
            System.out.println("created: " + created);

            ClientMessage msg = new ClientMessage("Hello, Java!".getBytes()).withPriority(5);
            PublishResult result = client.publish("example-queue", msg);
            System.out.println("published: " + result.getId());

            Delivery d = client.consume("example-queue");
            if (d != null) {
                System.out.println("got: " + new String(d.getMessage().getPayload()));
                client.ack("example-queue", d.getDeliveryTag());
            }

            System.out.println("Done!");
        }
    }
}
