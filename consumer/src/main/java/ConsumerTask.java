import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;

public class ConsumerTask implements Runnable {

    private final Connection connection;
    private final String queueName;
    private final String roomId;
    private final RoomManager roomManager;
    private final AtomicLong messagesProcessed;
    private final AtomicLong messagesFailed;
    private final Gson gson = new Gson();
    private final int prefetchCount;

    public ConsumerTask(Connection connection,
                        String queueName,
                        String roomId,
                        RoomManager roomManager,
                        AtomicLong messagesProcessed,
                        AtomicLong messagesFailed,
                        int prefetchCount) {
        this.connection        = connection;
        this.queueName         = queueName;
        this.roomId            = roomId;
        this.roomManager       = roomManager;
        this.messagesProcessed = messagesProcessed;
        this.messagesFailed    = messagesFailed;
        this.prefetchCount     = prefetchCount;
    }

    @Override
    public void run() {
        try {
            Channel channel = connection.createChannel();

            channel.basicQos(prefetchCount);

            System.out.println("[ConsumerTask] Room " + roomId
                    + " | queue=" + queueName
                    + " | prefetch=" + prefetchCount
                    + " | thread=" + Thread.currentThread().getName());

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                long deliveryTag = delivery.getEnvelope().getDeliveryTag();
                try {
                    String json = new String(delivery.getBody(), StandardCharsets.UTF_8);
                    ChatMessage msg = gson.fromJson(json, ChatMessage.class);

                    if (msg == null || msg.roomId == null) {
                        channel.basicNack(deliveryTag, false, false);
                        messagesFailed.incrementAndGet();
                        return;
                    }

                    roomManager.broadcastToRoom(msg.roomId, json);

                    channel.basicAck(deliveryTag, false);
                    messagesProcessed.incrementAndGet();

                } catch (Exception e) {
                    System.err.println("[ConsumerTask] Error in room "
                            + roomId + ": " + e.getMessage());
                    try {
                        channel.basicNack(deliveryTag, false, false);
                    } catch (Exception nackEx) {
                        System.err.println("[ConsumerTask] NACK failed: " + nackEx.getMessage());
                    }
                    messagesFailed.incrementAndGet();
                }
            };

            channel.basicConsume(queueName, false, deliverCallback, consumerTag -> {
                System.out.println("[ConsumerTask] Consumer cancelled for room " + roomId);
            });

        } catch (Exception e) {
            System.err.println("[ConsumerTask] Fatal error in room "
                    + roomId + ": " + e.getMessage());
            e.printStackTrace();
        }
    }
}