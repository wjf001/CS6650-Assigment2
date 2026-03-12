import com.rabbitmq.client.Connection;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class ConsumerPool {

    private final Connection connection;
    private final RoomManager roomManager;
    private final int numRooms;
    private final AtomicLong messagesProcessed;
    private final AtomicLong messagesFailed;
    private final int prefetchCount;

    private ExecutorService threadPool;

    private final ConcurrentHashMap<String, AtomicInteger> consumersPerRoom
            = new ConcurrentHashMap<>();

    private final AtomicInteger totalConsumers = new AtomicInteger(0);

    public ConsumerPool(Connection connection,
                        RoomManager roomManager,
                        int numRooms,
                        AtomicLong messagesProcessed,
                        AtomicLong messagesFailed,
                        int prefetchCount) {
        this.connection        = connection;
        this.roomManager       = roomManager;
        this.numRooms          = numRooms;
        this.messagesProcessed = messagesProcessed;
        this.messagesFailed    = messagesFailed;
        this.prefetchCount     = prefetchCount;
    }


    public void start(int threadsPerRoom) {
        int totalThreads = numRooms * threadsPerRoom;
        this.threadPool = Executors.newCachedThreadPool();

        for (int room = 1; room <= numRooms; room++) {
            String roomId = String.valueOf(room);
            consumersPerRoom.put(roomId, new AtomicInteger(0));

            for (int t = 0; t < threadsPerRoom; t++) {
                spawnConsumer(roomId);
            }
        }

        System.out.println("[ConsumerPool] Started " + totalThreads + " consumers ("
                + threadsPerRoom + " per room, " + numRooms + " rooms)"
                + " | prefetch=" + prefetchCount);
    }

    public void addConsumersForRoom(String roomId, int count) {
        for (int i = 0; i < count; i++) {
            spawnConsumer(roomId);
        }
        System.out.println("[ConsumerPool] Scaled UP room " + roomId
                + " by " + count + " consumers (now "
                + consumersPerRoom.get(roomId).get() + ")");
    }

    private void spawnConsumer(String roomId) {
        String queueName = "room-queue-" + roomId;
        ConsumerTask task = new ConsumerTask(
                connection, queueName, roomId,
                roomManager, messagesProcessed, messagesFailed, prefetchCount
        );
        threadPool.submit(task);
        consumersPerRoom.computeIfAbsent(roomId, k -> new AtomicInteger(0)).incrementAndGet();
        totalConsumers.incrementAndGet();
    }

    public int getConsumerCount(String roomId) {
        AtomicInteger count = consumersPerRoom.get(roomId);
        return count != null ? count.get() : 0;
    }

    public int getTotalConsumers() {
        return totalConsumers.get();
    }

    public void shutdown() {
        if (threadPool != null) {
            threadPool.shutdownNow();
            System.out.println("[ConsumerPool] Shut down.");
        }
    }
}