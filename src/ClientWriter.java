
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.BuiltinExchangeType;

public class ClientWriter {

    static int ligne=1;
    private static final String BROADCAST_EXCHANGE = "BroadcastToReplicas"; // The exchange name

    public static void main(String[] args) {
        // Setup connection and channel with RabbitMQ server
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost"); // Change this if your RabbitMQ server is not on localhost

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            // Declare a fanout exchange
            channel.exchangeDeclare(BROADCAST_EXCHANGE, BuiltinExchangeType.FANOUT);

            // Prepare the message to be sent
            String message = "Ligne "+ligne;
            ligne++;
            if (args.length > 0) {
                message = args[0]; // Optionally allow passing a message from the command line
            }

            // Publish the message to the fanout exchange
            channel.basicPublish(BROADCAST_EXCHANGE, "", null, message.getBytes("UTF-8"));
            System.out.println(" [x] Sent '" + message + "' to all replicas");

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Failed to connect to RabbitMQ or send message");
        }
    }
}
