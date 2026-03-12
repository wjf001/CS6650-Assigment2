import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ChatClient {

    // Server
    private static final String SERVER_URL   = "ws://chat-alb-1288559855.us-west-2.elb.amazonaws.com:8080";
    // Consumer
    private static final String CONSUMER_URL = "ws://52.13.118.239:8080";

    private static final int MSG_COUNT        = 500000;
    private static final int THREAD_POOL_SIZE = 20;   // 1 thread per room
    private static final int NUM_ROOMS        = 20;
    private static final int STUCK_TIMEOUT_S  = 120;

    // Counters
    public static AtomicInteger receivedCount = new AtomicInteger(0);
    public static AtomicInteger sentCount     = new AtomicInteger(0);
    public static AtomicInteger failedCount   = new AtomicInteger(0);

    public static void main(String[] args) throws Exception {

        BlockingQueue<ChatMessage> queue = new LinkedBlockingQueue<>();
        prepareMessageData(queue);

        CountDownLatch senderLatch = new CountDownLatch(THREAD_POOL_SIZE);
        ExecutorService workerPool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        Map<Integer, List<ChatClientTask>> roomTasks = new HashMap<>();

        long startTimestamp = System.currentTimeMillis();
        System.out.println("=== Two-Connection Architecture ===");
        System.out.println("Send to (ALB/Server): " + SERVER_URL);
        System.out.println("Receive from (Consumer): " + CONSUMER_URL);
        System.out.println("Threads: " + THREAD_POOL_SIZE + " | Messages: " + MSG_COUNT);
        System.out.println("===================================");

        // 1 thread per room
        for (int i = 0; i < THREAD_POOL_SIZE; i++) {
            int roomNum = (i % NUM_ROOMS) + 1;
            String serverFullUrl   = SERVER_URL + "/chat/" + roomNum;
            String consumerFullUrl = CONSUMER_URL + "/chat/" + roomNum;

            ChatClientTask worker = new ChatClientTask(
                    serverFullUrl, consumerFullUrl, queue, senderLatch
            );
            roomTasks.computeIfAbsent(roomNum, k -> new ArrayList<>()).add(worker);
            workerPool.submit(worker);
        }

        // Wait for all messages to be sent
        senderLatch.await();
        System.out.println("All messages sent. Waiting for responses...");

        // Wait for responses with stuck detection
        int lastCount    = 0;
        int stuckCounter = 0;

        while (receivedCount.get() < MSG_COUNT) {
            int current = receivedCount.get();
            System.out.println("Received: " + current + " / " + MSG_COUNT
                    + " (" + String.format("%.1f", (current * 100.0 / MSG_COUNT)) + "%)");

            if (current == lastCount) {
                stuckCounter++;
                if (stuckCounter >= STUCK_TIMEOUT_S) {
                    System.out.println("\nWARNING: No new responses for " + STUCK_TIMEOUT_S + "s, stopping wait.");
                    break;
                }
            } else {
                stuckCounter = 0;
                lastCount = current;
            }
            Thread.sleep(1000);
        }

        workerPool.shutdownNow();

        long endTimestamp = System.currentTimeMillis();
        long totalDuration = endTimestamp - startTimestamp;

        generateReport(roomTasks, totalDuration);
    }

    private static void prepareMessageData(BlockingQueue<ChatMessage> queue) {
        System.out.println("Preparing " + MSG_COUNT + " messages...");
        Random rand = new Random();
        String[] events = {"TEXT", "JOIN", "LEAVE"};
        for (int i = 0; i < MSG_COUNT; i++) {
            String uid = String.valueOf(rand.nextInt(100000) + 1);
            queue.add(new ChatMessage(uid, "User" + uid, "Msg #" + i, events[rand.nextInt(3)]));
        }
        System.out.println("Messages ready.");
    }

    private static void generateReport(Map<Integer, List<ChatClientTask>> roomTasks,
                                       long totalTime) throws Exception {
        List<Long> allLatencies = new ArrayList<>();
        Map<String, Integer> typeCounts = new HashMap<>();
        typeCounts.put("TEXT",  0);
        typeCounts.put("JOIN",  0);
        typeCounts.put("LEAVE", 0);

        PrintWriter fileWriter = new PrintWriter(new FileWriter("results.csv"));
        fileWriter.println("StartTime,Latency,Type");

        for (List<ChatClientTask> tasks : roomTasks.values()) {
            for (ChatClientTask task : tasks) {
                for (String[] record : task.results) {
                    fileWriter.println(String.join(",", record));
                    allLatencies.add(Long.parseLong(record[1]));
                    String type = record[2];
                    typeCounts.put(type, typeCounts.getOrDefault(type, 0) + 1);
                }
            }
        }
        fileWriter.close();

        int totalSent     = sentCount.get();
        int totalReceived = receivedCount.get();
        int totalFailed   = failedCount.get();
        double deliveryRate = totalSent > 0 ? (totalReceived * 100.0 / totalSent) : 0;

        System.out.println("\n===========================================");
        System.out.println("         Statistical Analysis Report");
        System.out.println("===========================================");
        System.out.printf("Total Duration:      %d ms%n", totalTime);
        System.out.printf("Messages Sent:       %d%n", totalSent);
        System.out.printf("Messages Received:   %d%n", totalReceived);
        System.out.printf("Messages Failed:     %d%n", totalFailed);
        System.out.printf("Delivery Rate:       %.1f%%%n", deliveryRate);

        if (!allLatencies.isEmpty()) {
            Collections.sort(allLatencies);
            double mean   = allLatencies.stream().mapToLong(v -> v).average().orElse(0);
            long median   = allLatencies.get(allLatencies.size() / 2);
            long p95      = allLatencies.get((int)(allLatencies.size() * 0.95));
            long p99      = allLatencies.get((int)(allLatencies.size() * 0.99));
            long min      = allLatencies.get(0);
            long max      = allLatencies.get(allLatencies.size() - 1);
            double throughput = (double) allLatencies.size() / (totalTime / 1000.0);

            System.out.printf("Total Throughput:    %.2f msg/sec%n", throughput);
            System.out.println("-------------------------------------------");
            System.out.printf("Mean Latency:        %.2f ms%n", mean);
            System.out.printf("Median Latency:      %d ms%n", median);
            System.out.printf("95th %% Latency:      %d ms%n", p95);
            System.out.printf("99th %% Latency:      %d ms%n", p99);
            System.out.printf("Min Latency:         %d ms%n", min);
            System.out.printf("Max Latency:         %d ms%n", max);
        }

        System.out.println("-------------------------------------------");
        System.out.println("Message Distribution:");
        System.out.println("  TEXT:  " + typeCounts.get("TEXT"));
        System.out.println("  JOIN:  " + typeCounts.get("JOIN"));
        System.out.println("  LEAVE: " + typeCounts.get("LEAVE"));
        System.out.println("-------------------------------------------");
        System.out.println("Throughput Per Room:");

        List<Integer> sortedRooms = new ArrayList<>(roomTasks.keySet());
        Collections.sort(sortedRooms);
        for (Integer roomId : sortedRooms) {
            int roomMsgCount = 0;
            for (ChatClientTask t : roomTasks.get(roomId)) roomMsgCount += t.results.size();
            double roomThroughput = (double) roomMsgCount / (totalTime / 1000.0);
            System.out.printf("  Room %2d: %.2f msg/sec%n", roomId, roomThroughput);
        }
        System.out.println("===========================================");
    }
}