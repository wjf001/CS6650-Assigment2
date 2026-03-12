import org.java_websocket.WebSocket;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class RoomManager {

    private final ConcurrentHashMap<String, Set<WebSocket>> roomSessions
            = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<WebSocket, String> sessionToRoom
            = new ConcurrentHashMap<>();

    private final AtomicLong totalBroadcasts = new AtomicLong(0);
    private final AtomicLong totalDeliveries = new AtomicLong(0);
    private final AtomicLong failedDeliveries = new AtomicLong(0);

    private final int numRooms;

    public RoomManager(int numRooms) {
        this.numRooms = numRooms;
        for (int i = 1; i <= numRooms; i++) {
            roomSessions.put(String.valueOf(i), ConcurrentHashMap.newKeySet());
        }
    }

    public void addClient(String roomId, WebSocket conn) {
        roomSessions.computeIfAbsent(roomId, k -> ConcurrentHashMap.newKeySet()).add(conn);
        sessionToRoom.put(conn, roomId);
    }

    public void removeClient(WebSocket conn) {
        String roomId = sessionToRoom.remove(conn);
        if (roomId != null) {
            Set<WebSocket> sessions = roomSessions.get(roomId);
            if (sessions != null) {
                sessions.remove(conn);
            }
        }
    }

    public int broadcastToRoom(String roomId, String jsonMessage) {
        Set<WebSocket> clients = roomSessions.get(roomId);
        if (clients == null || clients.isEmpty()) {
            totalBroadcasts.incrementAndGet();
            return 0;
        }

        int sent = 0;
        for (WebSocket client : clients) {
            if (client.isOpen()) {
                try {
                    client.send(jsonMessage);
                    sent++;
                    totalDeliveries.incrementAndGet();
                } catch (Exception e) {
                    failedDeliveries.incrementAndGet();
                }
            }
        }
        totalBroadcasts.incrementAndGet();
        return sent;
    }

    public String getRoomForClient(WebSocket conn) {
        return sessionToRoom.get(conn);
    }

    public int getTotalClients() {
        return roomSessions.values().stream().mapToInt(Set::size).sum();
    }

    public int getRoomSize(String roomId) {
        Set<WebSocket> sessions = roomSessions.get(roomId);
        return sessions != null ? sessions.size() : 0;
    }

    public long getTotalBroadcasts()   { return totalBroadcasts.get(); }
    public long getTotalDeliveries()   { return totalDeliveries.get(); }
    public long getFailedDeliveries()  { return failedDeliveries.get(); }

    public ConcurrentHashMap<String, Set<WebSocket>> getRoomSessions() {
        return roomSessions;
    }
}