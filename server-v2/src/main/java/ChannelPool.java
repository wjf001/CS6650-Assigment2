import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ChannelPool {
    private final BlockingQueue<Channel> pool;
    private final Connection connection;
    private final int poolSize;

    public ChannelPool(Connection connection, int poolSize) {
        this.connection = connection;
        this.poolSize = poolSize;
        this.pool = new LinkedBlockingQueue<>(poolSize);
    }

    public void init() throws Exception {
        for (int i = 0; i < poolSize; i++) {
            Channel channel = connection.createChannel();
            channel.exchangeDeclare("chat.exchange", "topic", true);
            pool.add(channel);
        }
        System.out.println("[ChannelPool] Initialized " + poolSize + " channels.");
    }

    public Channel borrowChannel() throws InterruptedException {
        return pool.take();
    }

    public void returnChannel(Channel channel) {
        if (channel != null && channel.isOpen()) {
            pool.offer(channel);
        } else {
            // Channel is dead — try to create a replacement
            try {
                Channel replacement = connection.createChannel();
                replacement.exchangeDeclare("chat.exchange", "topic", true);
                pool.offer(replacement);
                System.out.println("[ChannelPool] Replaced dead channel.");
            } catch (Exception e) {
                System.err.println("[ChannelPool] Failed to replace channel: " + e.getMessage());
            }
        }
    }

    public void close() {
        for (Channel ch : pool) {
            try { ch.close(); } catch (Exception ignored) {}
        }
    }
}