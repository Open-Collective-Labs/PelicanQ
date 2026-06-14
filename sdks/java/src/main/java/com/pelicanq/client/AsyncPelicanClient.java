package com.pelicanq.client;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.pelicanq.client.types.*;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class AsyncPelicanClient {

    private final ManagedChannel channel;
    private final pelicanq.v1.QueueServiceGrpc.QueueServiceFutureStub futureStub;
    private final pelicanq.v1.AdminServiceGrpc.AdminServiceFutureStub adminFutureStub;

    AsyncPelicanClient(ManagedChannel channel) {
        this.channel = channel;
        this.futureStub = pelicanq.v1.QueueServiceGrpc.newFutureStub(channel);
        this.adminFutureStub = pelicanq.v1.AdminServiceGrpc.newFutureStub(channel);
    }

    private static <T> CompletableFuture<T> toCompletableFuture(ListenableFuture<T> listenableFuture) {
        CompletableFuture<T> completable = new CompletableFuture<>();
        Futures.addCallback(listenableFuture, new FutureCallback<T>() {
            @Override
            public void onSuccess(T result) {
                completable.complete(result);
            }

            @Override
            public void onFailure(Throwable t) {
                if (t instanceof StatusRuntimeException) {
                    completable.completeExceptionally(new PelicanException("RPC failed", t));
                } else {
                    completable.completeExceptionally(t);
                }
            }
        }, MoreExecutors.directExecutor());
        return completable;
    }

    public CompletableFuture<Boolean> declareQueue(String name, QueueOptions options) {
        pelicanq.v1.DeclareQueueRequest.Builder req = pelicanq.v1.DeclareQueueRequest.newBuilder()
            .setName(name);
        if (options.getMaxAgeSecs() != null) req.setMaxAgeSecs(options.getMaxAgeSecs());
        if (options.getMaxMessages() != null) req.setMaxMessages(options.getMaxMessages());
        if (options.getMaxDeliveryAttempts() != null) req.setMaxDeliveryAttempts(options.getMaxDeliveryAttempts());
        if (options.getDeadLetterQueue() != null) req.setDeadLetterQueue(options.getDeadLetterQueue());
        if (options.getDedupWindowSecs() != null) req.setDedupWindowSecs(options.getDedupWindowSecs());
        ListenableFuture<pelicanq.v1.DeclareQueueResponse> future = futureStub.declareQueue(req.build());
        return toCompletableFuture(future).thenApply(pelicanq.v1.DeclareQueueResponse::getCreated);
    }

    public CompletableFuture<PublishResult> publish(String queue, ClientMessage msg) {
        PelicanClient client = new PelicanClient(channel);
        pelicanq.v1.PublishRequest req = pelicanq.v1.PublishRequest.newBuilder()
            .setQueue(queue)
            .setMessage(client.toProtoMessage(msg))
            .build();
        ListenableFuture<pelicanq.v1.PublishResponse> future = futureStub.publish(req);
        return toCompletableFuture(future)
            .thenApply(r -> new PublishResult(r.getId(), r.getDeduplicated()));
    }

    public CompletableFuture<List<PublishResult>> publishBatch(String queue, List<ClientMessage> messages) {
        PelicanClient client = new PelicanClient(channel);
        pelicanq.v1.PublishBatchRequest req = pelicanq.v1.PublishBatchRequest.newBuilder()
            .setQueue(queue)
            .addAllMessages(messages.stream().map(client::toProtoMessage).collect(Collectors.toList()))
            .build();
        ListenableFuture<pelicanq.v1.PublishBatchResponse> future = futureStub.publishBatch(req);
        return toCompletableFuture(future)
            .thenApply(res -> res.getResultsList().stream()
                .map(r -> new PublishResult(r.getId(), r.getDeduplicated()))
                .collect(Collectors.toList()));
    }

    public CompletableFuture<Delivery> consume(String queue) {
        pelicanq.v1.ConsumeRequest req = pelicanq.v1.ConsumeRequest.newBuilder()
            .setQueue(queue).build();
        ListenableFuture<pelicanq.v1.ConsumeResponse> future = futureStub.consume(req);
        return toCompletableFuture(future).thenApply(res -> {
            if (res.hasMessage()) {
                PelicanClient client = new PelicanClient(channel);
                return new Delivery(res.getMessage().getDeliveryTag(),
                    client.toClientMessage(res.getMessage().getMessage()));
            }
            return null;
        });
    }

    public CompletableFuture<List<Delivery>> consumeBatch(String queue, int max) {
        pelicanq.v1.ConsumeBatchRequest req = pelicanq.v1.ConsumeBatchRequest.newBuilder()
            .setQueue(queue).setMax(max).build();
        ListenableFuture<pelicanq.v1.ConsumeBatchResponse> future = futureStub.consumeBatch(req);
        return toCompletableFuture(future).thenApply(res -> {
            PelicanClient client = new PelicanClient(channel);
            return res.getMessagesList().stream()
                .map(cm -> new Delivery(cm.getDeliveryTag(), client.toClientMessage(cm.getMessage())))
                .collect(Collectors.toList());
        });
    }

    public CompletableFuture<Void> ack(String queue, long deliveryTag) {
        pelicanq.v1.AckRequest req = pelicanq.v1.AckRequest.newBuilder()
            .setQueue(queue).setDeliveryTag(deliveryTag).build();
        ListenableFuture<pelicanq.v1.AckResponse> future = futureStub.ack(req);
        return toCompletableFuture(future).thenApply(r -> null);
    }

    public CompletableFuture<Void> nack(String queue, long deliveryTag) {
        pelicanq.v1.NackRequest req = pelicanq.v1.NackRequest.newBuilder()
            .setQueue(queue).setDeliveryTag(deliveryTag).build();
        ListenableFuture<pelicanq.v1.NackResponse> future = futureStub.nack(req);
        return toCompletableFuture(future).thenApply(r -> null);
    }

    public CompletableFuture<List<QueueInfo>> listQueues() {
        ListenableFuture<pelicanq.v1.ListQueuesResponse> future = futureStub.listQueues(
            pelicanq.v1.ListQueuesRequest.newBuilder().build());
        return toCompletableFuture(future)
            .thenApply(res -> res.getQueuesList().stream()
                .map(q -> new QueueInfo(q.getName(), q.getDepth(), q.getScheduledDepth()))
                .collect(Collectors.toList()));
    }

    public CompletableFuture<String> health() {
        ListenableFuture<pelicanq.v1.HealthResponse> future = adminFutureStub.health(
            pelicanq.v1.HealthRequest.newBuilder().build());
        return toCompletableFuture(future)
            .thenApply(pelicanq.v1.HealthResponse::getStatus);
    }

    public CompletableFuture<ClusterStatus> clusterStatus() {
        ListenableFuture<pelicanq.v1.ClusterStatusResponse> future = adminFutureStub.clusterStatus(
            pelicanq.v1.ClusterStatusRequest.newBuilder().build());
        return toCompletableFuture(future)
            .thenApply(res -> {
                List<ClusterMember> members = res.getMembersList().stream()
                    .map(m -> new ClusterMember(m.getId(), m.getRaftAddr(), m.getClientAddr()))
                    .collect(Collectors.toList());
                return new ClusterStatus(
                    res.getSelfId(),
                    res.getIsLeader(),
                    res.hasCurrentLeaderId() ? res.getCurrentLeaderId() : null,
                    members);
            });
    }
}
