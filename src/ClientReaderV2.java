import com.rabbitmq.client.*;

import java.util.concurrent.CountDownLatch;

public class ClientReaderV2 {
    private static final String REQUEST_EXCHANGE = "RequestLastLine";
    private static final String RESPONSE_EXCHANGE = "ReplicaResponses";
    private static final String RESPONSE_QUEUE = "ResponseQueue";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        final CountDownLatch latch = new CountDownLatch(1);

        Connection connection = factory.newConnection();
             Channel channel = connection.createChannel();

            // Declare the direct exchange for responses
            channel.exchangeDeclare(RESPONSE_EXCHANGE, BuiltinExchangeType.DIRECT);

            // Declare and bind the queue for responses
            channel.queueDeclare(RESPONSE_QUEUE, false, false, false, null);
            channel.queueBind(RESPONSE_QUEUE, RESPONSE_EXCHANGE, RESPONSE_QUEUE);

            // Set up consumer to listen on the response queue
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String messageReceived = new String(delivery.getBody(), "UTF-8");
                System.out.println("Received: " + messageReceived);

            };

            channel.basicConsume(RESPONSE_QUEUE, true, deliverCallback, consumerTag -> {});

            // Declare the fanout exchange for requests and publish a request
            channel.exchangeDeclare(REQUEST_EXCHANGE, BuiltinExchangeType.FANOUT);
            String message = "READALL";
            channel.basicPublish(REQUEST_EXCHANGE, "", null, message.getBytes("UTF-8"));
            System.out.println(" [x] Sent request '" + message + "'");


    }
}
