import org.java_websocket.server.WebSocketServer;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import com.google.gson.Gson;
import com.rabbitmq.client.*;
import com.sun.net.httpserver.HttpServer;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class ChatServer extends WebSocketServer {

    private static final String RABBITMQ_HOST     = "172.31.21.136";
    private static final String EXCHANGE_NAME     = "chat.exchange";
    private static final int    NUM_ROOMS         = 20;
    private static final int    CHANNEL_POOL_SIZE = 50;
    private static final int    HEALTH_CHECK_PORT = 8081;

    private final Gson gson = new Gson();
    private ChannelPool channelPool;
    private String serverId;
    private Connection rabbitConnection;

    private final AtomicLong messagesPublished = new AtomicLong(0);
    private final AtomicLong messagesFailed    = new AtomicLong(0);
    private final AtomicLong connectionsActive = new AtomicLong(0);

    public ChatServer(int port) {
        super(new InetSocketAddress(port));
    }


    private void initialize() throws Exception {
        this.serverId = java.net.InetAddress.getLocalHost().getHostAddress();

        // 1. Connect to RabbitMQ with auto-recovery
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(RABBITMQ_HOST);
        factory.setAutomaticRecoveryEnabled(true);
        factory.setNetworkRecoveryInterval(5000);
        this.rabbitConnection = factory.newConnection();
        System.out.println("[ChatServer] Connected to RabbitMQ at " + RABBITMQ_HOST);

        // 2. Declare exchange, queues
        declareRoomQueues();

        // 3. Initialize producer channel pool
        this.channelPool = new ChannelPool(rabbitConnection, CHANNEL_POOL_SIZE);
        this.channelPool.init();

        // 4. Health check + metrics
        startHealthCheckServer();
        startMetricsReporter();

        System.out.println("[ChatServer] Fully initialized. ServerId=" + serverId);
    }

    private void declareRoomQueues() throws Exception {
        Channel setupChannel = rabbitConnection.createChannel();
        setupChannel.exchangeDeclare(EXCHANGE_NAME, "topic", true);

        // Dead Letter Exchange + Queue
        String dlxExchange = "chat.dlx";
        String dlqQueue    = "chat.dead-letter-queue";
        setupChannel.exchangeDeclare(dlxExchange, "fanout", true);
        setupChannel.queueDeclare(dlqQueue, true, false, false, null);
        setupChannel.queueBind(dlqQueue, dlxExchange, "");

        for (int i = 1; i <= NUM_ROOMS; i++) {
            String queueName  = "room-queue-" + i;
            String routingKey = "room." + i;

            Map<String, Object> queueArgs = new HashMap<>();
            queueArgs.put("x-dead-letter-exchange", dlxExchange);

            setupChannel.queueDeclare(queueName, true, false, false, queueArgs);
            setupChannel.queueBind(queueName, EXCHANGE_NAME, routingKey);
        }
        setupChannel.close();
        System.out.println("[ChatServer] All " + NUM_ROOMS + " queues declared with DLQ.");
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
            conn.close(1002, "Missing roomId in path");
            return;
        }

        conn.setAttachment(roomId);
        connectionsActive.incrementAndGet();
        System.out.println("[ChatServer] Client connected to room " + roomId
                + " from " + conn.getRemoteSocketAddress());
    }

    @Override
    public void onClose(WebSocket conn, int code, String reason, boolean remote) {
        connectionsActive.decrementAndGet();
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
            msg.serverId  = serverId;
            msg.clientIp  = conn.getRemoteSocketAddress().getAddress().getHostAddress();
            msg.timestamp = Instant.now().toString();

            // Publish to RabbitMQ — Consumer app will pick up and broadcast
            String routingKey   = "room." + roomId;
            String enrichedJson = gson.toJson(msg);

            Channel channel = channelPool.borrowChannel();
            try {
                channel.basicPublish(
                        EXCHANGE_NAME,
                        routingKey,
                        MessageProperties.PERSISTENT_TEXT_PLAIN,
                        enrichedJson.getBytes(StandardCharsets.UTF_8)
                );
                messagesPublished.incrementAndGet();
            } finally {
                channelPool.returnChannel(channel);
            }

        } catch (Exception e) {
            System.err.println("[ChatServer] Error publishing message: " + e.getMessage());
            messagesFailed.incrementAndGet();
        }
    }

    @Override
    public void onError(WebSocket conn, Exception ex) {
        System.err.println("[ChatServer] WebSocket error: " + ex.getMessage());
    }

    @Override
    public void onStart() {
        System.out.println("[ChatServer] WebSocket server listening on port 8080");
    }


    private void startHealthCheckServer() {
        Thread healthThread = new Thread(() -> {
            try {
                HttpServer httpServer = HttpServer.create(new InetSocketAddress(HEALTH_CHECK_PORT), 0);
                httpServer.createContext("/health", exchange -> {
                    String status = String.format(
                            "{\"status\":\"UP\",\"serverId\":\"%s\","
                                    + "\"published\":%d,\"failed\":%d,\"connections\":%d}",
                            serverId, messagesPublished.get(),
                            messagesFailed.get(), connectionsActive.get()
                    );
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(200, status.getBytes().length);
                    exchange.getResponseBody().write(status.getBytes());
                    exchange.getResponseBody().close();
                });
                httpServer.setExecutor(Executors.newSingleThreadExecutor());
                httpServer.start();
                System.out.println("[ChatServer] Health check on port " + HEALTH_CHECK_PORT);
            } catch (Exception e) {
                System.err.println("[ChatServer] Health check server failed: " + e.getMessage());
            }
        });
        healthThread.setDaemon(true);
        healthThread.start();
    }

    private void startMetricsReporter() {
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
            System.out.printf("[ChatServer] Metrics | published=%d | failed=%d | connections=%d%n",
                    messagesPublished.get(), messagesFailed.get(), connectionsActive.get());
        }, 5, 5, TimeUnit.SECONDS);
    }


    public static void main(String[] args) throws Exception {
        ChatServer server = new ChatServer(8080);
        server.initialize();
        server.start();
        System.out.println("[ChatServer] Server is running. Press Ctrl+C to stop.");
        Thread.currentThread().join();
    }
}