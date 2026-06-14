package com.pelicanq.client;

import com.pelicanq.client.types.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.io.Closeable;
import java.util.List;
import java.util.stream.Collectors;

public class PelicanClient implements Closeable {

    private final ManagedChannel channel;
    private final pelicanq.v1.QueueServiceGrpc.QueueServiceBlockingStub blockingStub;
    private final pelicanq.v1.QueueServiceGrpc.QueueServiceStub asyncStub;
    private final pelicanq.v1.AdminServiceGrpc.AdminServiceBlockingStub adminBlockingStub;

    PelicanClient(ManagedChannel channel) {
        this.channel = channel;
        this.blockingStub = pelicanq.v1.QueueServiceGrpc.newBlockingStub(channel);
        this.asyncStub = pelicanq.v1.QueueServiceGrpc.newStub(channel);
        this.adminBlockingStub = pelicanq.v1.AdminServiceGrpc.newBlockingStub(channel);
    }

    public static PelicanClientBuilder forAddress(String host, int port) {
        return new PelicanClientBuilder(host, port);
    }

    public boolean declareQueue(String name, QueueOptions options) throws PelicanException {
        try {
            pelicanq.v1.DeclareQueueRequest.Builder req = pelicanq.v1.DeclareQueueRequest.newBuilder()
                .setName(name);
            if (options.getMaxAgeSecs() != null) req.setMaxAgeSecs(options.getMaxAgeSecs());
            if (options.getMaxMessages() != null) req.setMaxMessages(options.getMaxMessages());
            if (options.getMaxDeliveryAttempts() != null) req.setMaxDeliveryAttempts(options.getMaxDeliveryAttempts());
            if (options.getDeadLetterQueue() != null) req.setDeadLetterQueue(options.getDeadLetterQueue());
            if (options.getDedupWindowSecs() != null) req.setDedupWindowSecs(options.getDedupWindowSecs());
            pelicanq.v1.DeclareQueueResponse res = blockingStub.declareQueue(req.build());
            return res.getCreated();
        } catch (StatusRuntimeException e) {
            throw new PelicanException("declareQueue failed", e);
        }
    }

    public PublishResult publish(String queue, ClientMessage msg) throws PelicanException {
        try {
            pelicanq.v1.PublishResponse res = blockingStub.publish(
                pelicanq.v1.PublishRequest.newBuilder()
                    .setQueue(queue)
                    .setMessage(toProtoMessage(msg))
                    .build());
            return new PublishResult(res.getId(), res.getDeduplicated());
        } catch (StatusRuntimeException e) {
            throw new PelicanException("publish failed", e);
        }
    }

    public List<PublishResult> publishBatch(String queue, List<ClientMessage> messages) throws PelicanException {
        try {
            pelicanq.v1.PublishBatchResponse res = blockingStub.publishBatch(
                pelicanq.v1.PublishBatchRequest.newBuilder()
                    .setQueue(queue)
                    .addAllMessages(messages.stream().map(this::toProtoMessage).collect(Collectors.toList()))
                    .build());
            return res.getResultsList().stream()
                .map(r -> new PublishResult(r.getId(), r.getDeduplicated()))
                .collect(Collectors.toList());
        } catch (StatusRuntimeException e) {
            throw new PelicanException("publishBatch failed", e);
        }
    }

    public Delivery consume(String queue) throws PelicanException {
        try {
            pelicanq.v1.ConsumeResponse res = blockingStub.consume(
                pelicanq.v1.ConsumeRequest.newBuilder().setQueue(queue).build());
            if (res.hasMessage()) {
                return toDelivery(res.getMessage());
            }
            return null;
        } catch (StatusRuntimeException e) {
            throw new PelicanException("consume failed", e);
        }
    }

    public List<Delivery> consumeBatch(String queue, int max) throws PelicanException {
        try {
            pelicanq.v1.ConsumeBatchResponse res = blockingStub.consumeBatch(
                pelicanq.v1.ConsumeBatchRequest.newBuilder()
                    .setQueue(queue)
                    .setMax(max)
                    .build());
            return res.getMessagesList().stream()
                .map(this::toDelivery)
                .collect(Collectors.toList());
        } catch (StatusRuntimeException e) {
            throw new PelicanException("consumeBatch failed", e);
        }
    }

    public void ack(String queue, long deliveryTag) throws PelicanException {
        try {
            blockingStub.ack(
                pelicanq.v1.AckRequest.newBuilder()
                    .setQueue(queue)
                    .setDeliveryTag(deliveryTag)
                    .build());
        } catch (StatusRuntimeException e) {
            throw new PelicanException("ack failed", e);
        }
    }

    public void nack(String queue, long deliveryTag) throws PelicanException {
        try {
            blockingStub.nack(
                pelicanq.v1.NackRequest.newBuilder()
                    .setQueue(queue)
                    .setDeliveryTag(deliveryTag)
                    .build());
        } catch (StatusRuntimeException e) {
            throw new PelicanException("nack failed", e);
        }
    }

    public List<QueueInfo> listQueues() throws PelicanException {
        try {
            pelicanq.v1.ListQueuesResponse res = blockingStub.listQueues(
                pelicanq.v1.ListQueuesRequest.newBuilder().build());
            return res.getQueuesList().stream()
                .map(q -> new QueueInfo(q.getName(), q.getDepth(), q.getScheduledDepth()))
                .collect(Collectors.toList());
        } catch (StatusRuntimeException e) {
            throw new PelicanException("listQueues failed", e);
        }
    }

    public String health() throws PelicanException {
        try {
            pelicanq.v1.HealthResponse res = adminBlockingStub.health(
                pelicanq.v1.HealthRequest.newBuilder().build());
            return res.getStatus();
        } catch (StatusRuntimeException e) {
            throw new PelicanException("health check failed", e);
        }
    }

    public AsyncPelicanClient async() {
        return new AsyncPelicanClient(channel);
    }

    public ClusterStatus clusterStatus() throws PelicanException {
        try {
            pelicanq.v1.ClusterStatusResponse res = adminBlockingStub.clusterStatus(
                pelicanq.v1.ClusterStatusRequest.newBuilder().build());
            List<ClusterMember> members = res.getMembersList().stream()
                .map(m -> new ClusterMember(m.getId(), m.getRaftAddr(), m.getClientAddr()))
                .collect(Collectors.toList());
            return new ClusterStatus(
                res.getSelfId(),
                res.getIsLeader(),
                res.hasCurrentLeaderId() ? res.getCurrentLeaderId() : null,
                members);
        } catch (StatusRuntimeException e) {
            throw new PelicanException("clusterStatus failed", e);
        }
    }

    public StreamObserver<pelicanq.v1.ConsumeStreamAck> consumeStream(
            String queue, StreamObserver<Delivery> observer) {
        StreamObserver<pelicanq.v1.ConsumedMessage> protoObserver =
                new StreamObserver<pelicanq.v1.ConsumedMessage>() {
            @Override
            public void onNext(pelicanq.v1.ConsumedMessage value) {
                observer.onNext(toDelivery(value));
            }

            @Override
            public void onError(Throwable t) {
                observer.onError(t);
            }

            @Override
            public void onCompleted() {
                observer.onCompleted();
            }
        };
        StreamObserver<pelicanq.v1.ConsumeStreamAck> ackObserver =
                asyncStub.consumeStream(protoObserver);
        // Send initial message with queue name
        ackObserver.onNext(pelicanq.v1.ConsumeStreamAck.newBuilder().setQueue(queue).build());
        return ackObserver;
    }

    @Override
    public void close() {
        channel.shutdown();
        try {
            channel.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    pelicanq.v1.Message toProtoMessage(ClientMessage msg) {
        pelicanq.v1.Message.Builder b = pelicanq.v1.Message.newBuilder()
            .setPayload(com.google.protobuf.ByteString.copyFrom(msg.getPayload()))
            .putAllHeaders(msg.getHeaders())
            .setTimestamp(msg.getTimestamp())
            .setPriority(msg.getPriority())
            .setDeliveryAttempts(msg.getDeliveryAttempts());
        if (msg.getDeliverAt() != null) b.setDeliverAt(msg.getDeliverAt());
        if (msg.getDedupKey() != null) b.setDedupKey(msg.getDedupKey());
        return b.build();
    }

    private Delivery toDelivery(pelicanq.v1.ConsumedMessage cm) {
        return new Delivery(cm.getDeliveryTag(), toClientMessage(cm.getMessage()));
    }

    private ClientMessage toClientMessage(pelicanq.v1.Message m) {
        return new ClientMessage(m.getPayload().toByteArray())
            .withHeaders(m.getHeadersMap())
            .withTimestamp(m.getTimestamp())
            .withPriority(m.getPriority())
            .withDeliverAt(m.hasDeliverAt() ? m.getDeliverAt() : null)
            .withDedupKey(m.hasDedupKey() ? m.getDedupKey() : null)
            .withDeliveryAttempts(m.getDeliveryAttempts());
    }

    public static class PelicanClientBuilder {
        private final String host;
        private final int port;
        private boolean usePlaintext = true;

        PelicanClientBuilder(String host, int port) {
            this.host = host;
            this.port = port;
        }

        public PelicanClientBuilder usePlaintext(boolean plaintext) {
            this.usePlaintext = plaintext;
            return this;
        }

        public PelicanClient build() {
            ManagedChannelBuilder<?> builder = ManagedChannelBuilder.forAddress(host, port);
            if (usePlaintext) {
                builder.usePlaintext();
            }
            return new PelicanClient(builder.build());
        }
    }
}
