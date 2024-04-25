
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.BuiltinExchangeType;

import java.util.Scanner;

public class ClientWriter {

    static int ligne=1;
    private static final String BROADCAST_EXCHANGE = "BroadcastToReplicas"; // The exchange name

    public static void main(String[] args) {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try
        {
            Scanner scanner = new Scanner(System.in);
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();
            // Declare a fanout exchange
            channel.exchangeDeclare(BROADCAST_EXCHANGE, BuiltinExchangeType.FANOUT);

            // Prepare the message to be sent

            while (true) {
                String message = "Ligne "+ligne;
                ligne++;
                System.out.print("Press anything to send: ");
                String val = scanner.nextLine(); // Read message from the console
                if (val.equalsIgnoreCase("exit")) {
                    break; // Exit if the user types "exit"
                }

                // Publish the message to the fanout exchange
                channel.basicPublish(BROADCAST_EXCHANGE, "", null, message.getBytes("UTF-8"));
                System.out.println(" [x] Sent '" + message + "' to all replicas");
            }

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Failed to connect to RabbitMQ or send message");
        }
    }
}
