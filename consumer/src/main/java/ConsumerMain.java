import com.google.gson.Gson;
import com.rabbitmq.client.*;
import com.sun.net.httpserver.HttpServer;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class ConsumerMain extends WebSocketServer {

    private static final String RABBITMQ_HOST     = "172.31.21.136";
    private static final String EXCHANGE_NAME     = "chat.exchange";
    private static final int    NUM_ROOMS         = 20;
    private static final int    CHANNEL_POOL_SIZE = 50;
    private static final int    HEALTH_CHECK_PORT = 8081;

    private static final int THREADS_PER_ROOM = 4;
    private static final int PREFETCH_COUNT   = 25;

    private static final Gson gson = new Gson();
    private static String consumerId;
    private static Connection rabbitConnection;
    private static ChannelPool channelPool;

    private static RoomManager roomManager;
    private static ConsumerPool consumerPool;

    // Metrics
    public static final AtomicLong messagesProcessed = new AtomicLong(0);
    public static final AtomicLong messagesFailed    = new AtomicLong(0);
    public static final AtomicLong messagesPublished = new AtomicLong(0);

    public ConsumerMain(int port) {
        super(new InetSocketAddress(port));
    }

    @Override
    public void onOpen(WebSocket conn, ClientHandshake handshake) {
        String path = handshake.getResourceDescriptor();
        if (path == null || !path.startsWith("/chat/")) {
            conn.close(1002, "Invalid endpoint. Use /chat/{roomId}");
            return;
        }
        String roomId = path.substring(6).trim();
        if (roomId.isEmpty()) {
            conn.close(1002, "Missing roomId");
            return;
        }

        // Register in Room Manager
        conn.setAttachment(roomId);
        roomManager.addClient(roomId, conn);

        System.out.println("[Consumer] Client joined room " + roomId
                + " | room size=" + roomManager.getRoomSize(roomId));
    }

    @Override
    public void onClose(WebSocket conn, int code, String reason, boolean remote) {
        roomManager.removeClient(conn);
    }


    @Override
    public void onMessage(WebSocket conn, String rawMessage) {
        try {
            String roomId = conn.getAttachment();
            if (roomId == null) return;

            ChatMessage msg = gson.fromJson(rawMessage, ChatMessage.class);
            if (msg == null) return;

            msg.messageId = UUID.randomUUID().toString();
            msg.roomId    = roomId;
            msg.serverId  = consumerId;
            msg.clientIp  = conn.getRemoteSocketAddress().getAddress().getHostAddress();
            msg.timestamp = Instant.now().toString();

            // Publish to RabbitMQ
            Channel channel = channelPool.borrowChannel();
            try {
                channel.basicPublish(
                        EXCHANGE_NAME,
                        "room." + roomId,
                        MessageProperties.PERSISTENT_TEXT_PLAIN,
                        gson.toJson(msg).getBytes(StandardCharsets.UTF_8)
                );
                messagesPublished.incrementAndGet();
            } finally {
                channelPool.returnChannel(channel);
            }
        } catch (Exception e) {
            System.err.println("[Consumer] Error publishing: " + e.getMessage());
        }
    }

    @Override
    public void onError(WebSocket conn, Exception ex) {
        System.err.println("[Consumer] WS error: " + ex.getMessage());
    }

    @Override
    public void onStart() {
        System.out.println("[Consumer] WebSocket server listening on port 8080");
    }


    private static void declareQueues(Connection connection) throws Exception {
        Channel ch = connection.createChannel();
        ch.exchangeDeclare(EXCHANGE_NAME, "topic", true);

        String dlxExchange = "chat.dlx";
        String dlqQueue    = "chat.dead-letter-queue";
        ch.exchangeDeclare(dlxExchange, "fanout", true);
        ch.queueDeclare(dlqQueue, true, false, false, null);
        ch.queueBind(dlqQueue, dlxExchange, "");

        for (int i = 1; i <= NUM_ROOMS; i++) {
            String queueName  = "room-queue-" + i;
            String routingKey = "room." + i;

            Map<String, Object> queueArgs = new HashMap<>();
            queueArgs.put("x-dead-letter-exchange", dlxExchange);

            ch.queueDeclare(queueName, true, false, false, queueArgs);
            ch.queueBind(queueName, EXCHANGE_NAME, routingKey);
        }
        ch.close();
        System.out.println("[Consumer] All " + NUM_ROOMS + " queues declared with DLQ.");
    }


    private static void startHealthCheckServer() {
        Thread healthThread = new Thread(() -> {
            try {
                HttpServer httpServer = HttpServer.create(new InetSocketAddress(HEALTH_CHECK_PORT), 0);
                httpServer.createContext("/health", exchange -> {
                    String status = String.format(
                            "{\"status\":\"UP\",\"consumerId\":\"%s\","
                                    + "\"processed\":%d,\"published\":%d,\"failed\":%d,"
                                    + "\"clients\":%d,\"consumerThreads\":%d,"
                                    + "\"broadcasts\":%d,\"deliveries\":%d}",
                            consumerId, messagesProcessed.get(),
                            messagesPublished.get(), messagesFailed.get(),
                            roomManager.getTotalClients(),
                            consumerPool.getTotalConsumers(),
                            roomManager.getTotalBroadcasts(),
                            roomManager.getTotalDeliveries()
                    );
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(200, status.getBytes().length);
                    exchange.getResponseBody().write(status.getBytes());
                    exchange.getResponseBody().close();
                });
                httpServer.setExecutor(Executors.newSingleThreadExecutor());
                httpServer.start();
                System.out.println("[Consumer] Health check on port " + HEALTH_CHECK_PORT);
            } catch (Exception e) {
                System.err.println("[Consumer] Health check failed: " + e.getMessage());
            }
        });
        healthThread.setDaemon(true);
        healthThread.start();
    }

    private static void startMetricsReporter() {
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
            System.out.printf("[Consumer] Metrics | processed=%d | published=%d "
                            + "| failed=%d | clients=%d | consumers=%d%n",
                    messagesProcessed.get(), messagesPublished.get(),
                    messagesFailed.get(), roomManager.getTotalClients(),
                    consumerPool.getTotalConsumers());
        }, 5, 5, TimeUnit.SECONDS);
    }


    public static void main(String[] args) throws Exception {
        consumerId = java.net.InetAddress.getLocalHost().getHostAddress();
        System.out.println("[Consumer] Starting. ConsumerId=" + consumerId);

        // 1. Connect to RabbitMQ
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(RABBITMQ_HOST);
        factory.setAutomaticRecoveryEnabled(true);
        factory.setNetworkRecoveryInterval(5000);
        rabbitConnection = factory.newConnection();
        System.out.println("[Consumer] Connected to RabbitMQ at " + RABBITMQ_HOST);

        // 2. Declare queues with DLQ
        declareQueues(rabbitConnection);

        // 3. Initialize channel pool for publishing
        channelPool = new ChannelPool(rabbitConnection, CHANNEL_POOL_SIZE);
        channelPool.init();

        // 4. Initialize Room Manager
        roomManager = new RoomManager(NUM_ROOMS);
        System.out.println("[Consumer] Room Manager initialized for " + NUM_ROOMS + " rooms.");

        // 5. Start WebSocket server
        ConsumerMain app = new ConsumerMain(8080);
        app.start();

        // 6. Start Multi-threaded Consumer Pool
        consumerPool = new ConsumerPool(
                rabbitConnection, roomManager, NUM_ROOMS,
                messagesProcessed, messagesFailed, PREFETCH_COUNT
        );
        consumerPool.start(THREADS_PER_ROOM);

        // 7. Health check + metrics
        startHealthCheckServer();
        startMetricsReporter();

        System.out.println("[Consumer] Fully started. " + consumerPool.getTotalConsumers()
                + " consumer threads across " + NUM_ROOMS + " rooms.");
        Thread.currentThread().join();
    }
}