import com.rabbitmq.client.*;
import java.io.IOException;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.List;


public class Replica {
    private static final String BROADCAST_EXCHANGE = "BroadcastToReplicas";
    private static final String REQUEST_EXCHANGE = "RequestLastLine";
    private static final String RESPONSE_EXCHANGE = "ReplicaResponses";
    private static final String RESPONSE_QUEUE = "ResponseQueue"; // Queue for responses
    private static final String REQUEST_QUEUE = "ResquestQueue"; // Queue for responses

    public static void main(String[] argv) throws Exception {
        if (argv.length < 1) {
            System.err.println("Usage: java Replica [replica number]");
            System.exit(1);
        }

        final int replicaNumber = Integer.parseInt(argv[0]);
        final String replicaQueue = "Replica" + replicaNumber;
        final String fileName = "replicaFile" + replicaNumber + ".txt";

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel() ;

            // Declare the fanout exchange for broadcasts
            channel.exchangeDeclare(BROADCAST_EXCHANGE, BuiltinExchangeType.FANOUT);
            channel.queueDeclare(replicaQueue,false,false,false,null);
            channel.queueBind(replicaQueue, BROADCAST_EXCHANGE, "");


            System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
            // Set up consumers
            // Consumer for broadcast messages
            DeliverCallback broadcastCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), "UTF-8");
                try {
                    Files.write(Paths.get(fileName), (message + "\n").getBytes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                } catch (IOException e) {
                    e.printStackTrace();
                }

                System.out.println(" [x] Replica " + replicaNumber + " received broadcast: '" + message + "'");
            };
            channel.basicConsume(replicaQueue, true, broadcastCallback, consumerTag -> {});

            // Declare the fanout exchange for last line requests
            channel.exchangeDeclare(REQUEST_EXCHANGE, BuiltinExchangeType.FANOUT);
            String requestQueue = channel.queueDeclare().getQueue();
            channel.queueBind(requestQueue, REQUEST_EXCHANGE, "");
            // Consumer for "READLAST" requests
           DeliverCallback requestCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), "UTF-8");
                if (message.equals("READLAST")) {
                    try {
                        List<String> lines = Files.readAllLines(Paths.get(fileName));
                        if (!lines.isEmpty()) {
                            String lastLine = lines.get(lines.size() - 1);
                            channel.basicPublish(RESPONSE_EXCHANGE, RESPONSE_QUEUE, null, lastLine.getBytes("UTF-8"));
                            System.out.println(" [x] Replica " + replicaNumber + " responded with last line: '" + lastLine + "'");
                        }
                    } catch (IOException e) {
                        System.err.println("Failed to read from file: " + fileName);
                        e.printStackTrace();
                    }
                }
                else if (message.equals("READALL"))
               {
                   try {
                       List<String> lines = Files.readAllLines(Paths.get(fileName));
                       if (!lines.isEmpty()) {
                           for (int i = 0; i < lines.size() ; i++) {
                               String lastLine = lines.get(i);
                               channel.basicPublish(RESPONSE_EXCHANGE, RESPONSE_QUEUE, null, lastLine.getBytes("UTF-8"));
                               System.out.println(" [x] Replica " + replicaNumber + " responded with line:  " + i + "'" + lastLine + "'");
                           }
                       }
                   } catch (IOException e) {
                       System.err.println("Failed to read from file: " + fileName);
                       e.printStackTrace();
                   }
               }
            };
            channel.basicConsume(requestQueue, true, requestCallback, consumerTag -> {});


        }

    }

