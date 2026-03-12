import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import com.google.gson.Gson;
import java.net.URI;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.ArrayList;
import java.util.List;

public class ChatClientTask implements Runnable {

    private final String serverUri;
    private final String consumerUri;
    private final BlockingQueue<ChatMessage> queue;
    private final CountDownLatch latch;
    private final Gson gson = new Gson();

    private final ConcurrentHashMap<String, Long> startTimes = new ConcurrentHashMap<>();
    private final AtomicLong seqCounter = new AtomicLong(0);

    public final List<String[]> results = new ArrayList<>();
    public int messagesSent   = 0;
    public int messagesFailed = 0;

    public ChatClientTask(String serverUri, String consumerUri,
                          BlockingQueue<ChatMessage> queue, CountDownLatch latch) {
        this.serverUri   = serverUri;
        this.consumerUri = consumerUri;
        this.queue       = queue;
        this.latch       = latch;
    }

    @Override
    public void run() {
        WebSocketClient sendClient = null;
        WebSocketClient recvClient = null;
        try {
            recvClient = new WebSocketClient(new URI(consumerUri)) {
                @Override
                public void onOpen(ServerHandshake handshakedata) {
                    System.out.println("[RecvClient] Connected to consumer: " + consumerUri);
                }

                @Override
                public void onMessage(String message) {
                    long endTime = System.currentTimeMillis();
                    try {
                        ChatMessage response = gson.fromJson(message, ChatMessage.class);
                        if (response == null || response.username == null) return;

                        Long startTime = startTimes.remove(response.username);
                        if (startTime != null) {
                            long latency = endTime - startTime;
                            results.add(new String[]{
                                    String.valueOf(startTime),
                                    String.valueOf(latency),
                                    response.messageType != null ? response.messageType : "TEXT"
                            });
                            ChatClient.receivedCount.incrementAndGet();
                        }
                    } catch (Exception e) {
                    }
                }

                @Override
                public void onClose(int code, String reason, boolean remote) {}

                @Override
                public void onError(Exception ex) {}
            };
            recvClient.connectBlocking();

            sendClient = new WebSocketClient(new URI(serverUri)) {
                @Override
                public void onOpen(ServerHandshake handshakedata) {
                    System.out.println("[SendClient] Connected to server: " + serverUri);
                }

                @Override
                public void onMessage(String message) {
                }

                @Override
                public void onClose(int code, String reason, boolean remote) {}

                @Override
                public void onError(Exception ex) {}
            };
            sendClient.connectBlocking();

            while (true) {
                ChatMessage msg = queue.poll(1, java.util.concurrent.TimeUnit.SECONDS);
                if (msg == null) break;

                boolean sent    = false;
                int     retries = 0;
                long    backoff = 100;

                String seqId = msg.userId + "-" + seqCounter.incrementAndGet();
                msg.username = seqId;

                while (!sent && retries < 5) {
                    try {
                        long start = System.currentTimeMillis();
                        if (retries == 0) startTimes.put(seqId, start);

                        sendClient.send(gson.toJson(msg));
                        sent = true;
                        messagesSent++;
                        ChatClient.sentCount.incrementAndGet();

                    } catch (Exception e) {
                        retries++;
                        try { Thread.sleep(backoff); } catch (Exception ignored) {}
                        backoff *= 2;
                    }
                }

                if (!sent) {
                    messagesFailed++;
                    ChatClient.failedCount.incrementAndGet();
                }
            }

            Thread.sleep(120000);

            if (sendClient != null) sendClient.close();
            if (recvClient != null) recvClient.close();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            latch.countDown();
        }
    }
}