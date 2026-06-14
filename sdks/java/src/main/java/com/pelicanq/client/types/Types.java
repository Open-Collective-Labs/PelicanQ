package com.pelicanq.client.types;

import java.util.List;
import java.util.Map;
import java.util.HashMap;

public class ClientMessage {
    private final byte[] payload;
    private final Map<String, String> headers;
    private final long timestamp;
    private final int priority;
    private final Long deliverAt;
    private final String dedupKey;
    private final int deliveryAttempts;

    private ClientMessage(Builder builder) {
        this.payload = builder.payload;
        this.headers = builder.headers != null ? builder.headers : new HashMap<>();
        this.timestamp = builder.timestamp;
        this.priority = builder.priority;
        this.deliverAt = builder.deliverAt;
        this.dedupKey = builder.dedupKey;
        this.deliveryAttempts = builder.deliveryAttempts;
    }

    public ClientMessage(byte[] payload) {
        this.payload = payload;
        this.headers = new HashMap<>();
        this.timestamp = System.currentTimeMillis() / 1000;
        this.priority = 0;
        this.deliverAt = null;
        this.dedupKey = null;
        this.deliveryAttempts = 0;
    }

    public byte[] getPayload() { return payload; }
    public Map<String, String> getHeaders() { return headers; }
    public long getTimestamp() { return timestamp; }
    public int getPriority() { return priority; }
    public Long getDeliverAt() { return deliverAt; }
    public String getDedupKey() { return dedupKey; }
    public int getDeliveryAttempts() { return deliveryAttempts; }

    public Builder toBuilder() {
        return new Builder()
            .withPayload(payload)
            .withHeaders(headers)
            .withTimestamp(timestamp)
            .withPriority(priority)
            .withDeliverAt(deliverAt)
            .withDedupKey(dedupKey)
            .withDeliveryAttempts(deliveryAttempts);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public ClientMessage withPriority(int priority) {
        return toBuilder().withPriority(priority).build();
    }

    public ClientMessage withPayload(byte[] payload) {
        return toBuilder().withPayload(payload).build();
    }

    public ClientMessage withHeader(String key, String value) {
        Builder b = toBuilder();
        b.headers.put(key, value);
        return b.build();
    }

    public ClientMessage withHeaders(Map<String, String> headers) {
        return toBuilder().withHeaders(headers).build();
    }

    public ClientMessage withTimestamp(long timestamp) {
        return toBuilder().withTimestamp(timestamp).build();
    }

    public ClientMessage withDeliverAt(Long deliverAt) {
        return toBuilder().withDeliverAt(deliverAt).build();
    }

    public ClientMessage withDedupKey(String dedupKey) {
        return toBuilder().withDedupKey(dedupKey).build();
    }

    public ClientMessage withDeliveryAttempts(int deliveryAttempts) {
        return toBuilder().withDeliveryAttempts(deliveryAttempts).build();
    }

    public static class Builder {
        private byte[] payload;
        private Map<String, String> headers;
        private long timestamp;
        private int priority;
        private Long deliverAt;
        private String dedupKey;
        private int deliveryAttempts;

        public Builder withPayload(byte[] payload) { this.payload = payload; return this; }
        public Builder withHeaders(Map<String, String> headers) { this.headers = headers; return this; }
        public Builder withTimestamp(long timestamp) { this.timestamp = timestamp; return this; }
        public Builder withPriority(int priority) { this.priority = priority; return this; }
        public Builder withDeliverAt(Long deliverAt) { this.deliverAt = deliverAt; return this; }
        public Builder withDedupKey(String dedupKey) { this.dedupKey = dedupKey; return this; }
        public Builder withDeliveryAttempts(int deliveryAttempts) { this.deliveryAttempts = deliveryAttempts; return this; }

        public ClientMessage build() {
            return new ClientMessage(this);
        }
    }
}

public class PublishResult {
    private final String id;
    private final boolean deduplicated;

    public PublishResult(String id, boolean deduplicated) {
        this.id = id;
        this.deduplicated = deduplicated;
    }

    public String getId() { return id; }
    public boolean isDeduplicated() { return deduplicated; }
}

public class Delivery {
    private final long deliveryTag;
    private final ClientMessage message;

    public Delivery(long deliveryTag, ClientMessage message) {
        this.deliveryTag = deliveryTag;
        this.message = message;
    }

    public long getDeliveryTag() { return deliveryTag; }
    public ClientMessage getMessage() { return message; }
}

public class QueueOptions {
    private final Long maxAgeSecs;
    private final Long maxMessages;
    private final Integer maxDeliveryAttempts;
    private final String deadLetterQueue;
    private final Long dedupWindowSecs;

    public QueueOptions() {
        this.maxAgeSecs = null;
        this.maxMessages = null;
        this.maxDeliveryAttempts = null;
        this.deadLetterQueue = null;
        this.dedupWindowSecs = null;
    }

    public QueueOptions(Long maxAgeSecs, Long maxMessages, Integer maxDeliveryAttempts,
                        String deadLetterQueue, Long dedupWindowSecs) {
        this.maxAgeSecs = maxAgeSecs;
        this.maxMessages = maxMessages;
        this.maxDeliveryAttempts = maxDeliveryAttempts;
        this.deadLetterQueue = deadLetterQueue;
        this.dedupWindowSecs = dedupWindowSecs;
    }

    public Long getMaxAgeSecs() { return maxAgeSecs; }
    public Long getMaxMessages() { return maxMessages; }
    public Integer getMaxDeliveryAttempts() { return maxDeliveryAttempts; }
    public String getDeadLetterQueue() { return deadLetterQueue; }
    public Long getDedupWindowSecs() { return dedupWindowSecs; }
}

public class QueueInfo {
    private final String name;
    private final long depth;
    private final long scheduledDepth;

    public QueueInfo(String name, long depth, long scheduledDepth) {
        this.name = name;
        this.depth = depth;
        this.scheduledDepth = scheduledDepth;
    }

    public String getName() { return name; }
    public long getDepth() { return depth; }
    public long getScheduledDepth() { return scheduledDepth; }
}

public class PelicanException extends Exception {
    public PelicanException(String message) {
        super(message);
    }

    public PelicanException(String message, Throwable cause) {
        super(message, cause);
    }
}

public class ClusterMember {
    private final long id;
    private final String raftAddr;
    private final String clientAddr;

    public ClusterMember(long id, String raftAddr, String clientAddr) {
        this.id = id;
        this.raftAddr = raftAddr;
        this.clientAddr = clientAddr;
    }

    public long getId() { return id; }
    public String getRaftAddr() { return raftAddr; }
    public String getClientAddr() { return clientAddr; }
}

public class ClusterStatus {
    private final long selfId;
    private final boolean isLeader;
    private final Long currentLeaderId;
    private final List<ClusterMember> members;

    public ClusterStatus(long selfId, boolean isLeader, Long currentLeaderId, List<ClusterMember> members) {
        this.selfId = selfId;
        this.isLeader = isLeader;
        this.currentLeaderId = currentLeaderId;
        this.members = members;
    }

    public long getSelfId() { return selfId; }
    public boolean isLeader() { return isLeader; }
    public Long getCurrentLeaderId() { return currentLeaderId; }
    public List<ClusterMember> getMembers() { return members; }
}
