package com.pelicanq.client;

import com.pelicanq.client.types.*;
import org.junit.Test;
import static org.junit.Assert.*;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

public class PelicanClientTest {

    @Test
    public void testClientMessageBuilder() {
        ClientMessage msg = ClientMessage.newBuilder()
            .withPayload("hello".getBytes())
            .withPriority(5)
            .build();
        assertArrayEquals("hello".getBytes(), msg.getPayload());
        assertEquals(5, msg.getPriority());
        assertNotNull(msg.getHeaders());
        assertTrue(msg.getHeaders().isEmpty());
    }

    @Test
    public void testClientMessageWithMethods() {
        ClientMessage msg = new ClientMessage("data".getBytes())
            .withPriority(3)
            .withHeader("key", "val");
        assertEquals(3, msg.getPriority());
        assertEquals("val", msg.getHeaders().get("key"));
    }

    @Test
    public void testClientMessageWithHeaders() {
        Map<String, String> headers = new HashMap<>();
        headers.put("x-type", "test");
        ClientMessage msg = new ClientMessage("payload".getBytes())
            .withHeaders(headers);
        assertEquals("test", msg.getHeaders().get("x-type"));
    }

    @Test
    public void testClientMessageCustomTimestamp() {
        long ts = 1234567890L;
        ClientMessage msg = new ClientMessage("ts".getBytes()).withTimestamp(ts);
        assertEquals(ts, msg.getTimestamp());
    }

    @Test
    public void testClientMessageDedupKey() {
        ClientMessage msg = new ClientMessage("dedup".getBytes()).withDedupKey("dk-1");
        assertEquals("dk-1", msg.getDedupKey());
    }

    @Test
    public void testClientMessageDeliverAt() {
        ClientMessage msg = new ClientMessage("scheduled".getBytes()).withDeliverAt(999999L);
        assertEquals(Long.valueOf(999999L), msg.getDeliverAt());
    }

    @Test
    public void testClientMessageDeliveryAttempts() {
        ClientMessage msg = new ClientMessage("retry".getBytes()).withDeliveryAttempts(3);
        assertEquals(3, msg.getDeliveryAttempts());
    }

    @Test
    public void testPublishResult() {
        PublishResult pr = new PublishResult("msg-1", false);
        assertEquals("msg-1", pr.getId());
        assertFalse(pr.isDeduplicated());

        PublishResult pr2 = new PublishResult("msg-2", true);
        assertTrue(pr2.isDeduplicated());
    }

    @Test
    public void testDelivery() {
        ClientMessage inner = new ClientMessage("test".getBytes());
        Delivery d = new Delivery(42L, inner);
        assertEquals(42L, d.getDeliveryTag());
        assertSame(inner, d.getMessage());
    }

    @Test
    public void testQueueOptionsDefault() {
        QueueOptions opts = new QueueOptions();
        assertNull(opts.getMaxAgeSecs());
        assertNull(opts.getMaxMessages());
        assertNull(opts.getMaxDeliveryAttempts());
        assertNull(opts.getDeadLetterQueue());
        assertNull(opts.getDedupWindowSecs());
    }

    @Test
    public void testQueueOptionsCustom() {
        QueueOptions opts = new QueueOptions(
            3600L, 1000L, 5,
            "dlq", 300L);
        assertEquals(Long.valueOf(3600L), opts.getMaxAgeSecs());
        assertEquals(Long.valueOf(1000L), opts.getMaxMessages());
        assertEquals(Integer.valueOf(5), opts.getMaxDeliveryAttempts());
        assertEquals("dlq", opts.getDeadLetterQueue());
        assertEquals(Long.valueOf(300L), opts.getDedupWindowSecs());
    }

    @Test
    public void testQueueInfo() {
        QueueInfo qi = new QueueInfo("q1", 10, 2);
        assertEquals("q1", qi.getName());
        assertEquals(10, qi.getDepth());
        assertEquals(2, qi.getScheduledDepth());
    }

    @Test
    public void testPelicanException() {
        PelicanException e = new PelicanException("something went wrong");
        assertEquals("something went wrong", e.getMessage());

        PelicanException e2 = new PelicanException("wrapped", new RuntimeException("cause"));
        assertEquals("wrapped", e2.getMessage());
        assertNotNull(e2.getCause());
        assertEquals("cause", e2.getCause().getMessage());
    }

    @Test
    public void testPelicanClientBuilder() {
        PelicanClient.PelicanClientBuilder builder = PelicanClient.forAddress("localhost", 7072);
        assertNotNull(builder);
    }

    @Test
    public void testClientMessageToBuilder() {
        ClientMessage original = new ClientMessage("orig".getBytes())
            .withPriority(7)
            .withHeader("h1", "v1");
        ClientMessage copy = original.toBuilder().build();
        assertArrayEquals(original.getPayload(), copy.getPayload());
        assertEquals(original.getPriority(), copy.getPriority());
        assertEquals(original.getHeaders(), copy.getHeaders());
    }

    @Test
    public void testClusterMember() {
        ClusterMember m = new ClusterMember(1L, "10.0.0.1:7071", "10.0.0.1:7072");
        assertEquals(1L, m.getId());
        assertEquals("10.0.0.1:7071", m.getRaftAddr());
        assertEquals("10.0.0.1:7072", m.getClientAddr());
    }

    @Test
    public void testClusterStatus() {
        List<ClusterMember> members = new ArrayList<>();
        members.add(new ClusterMember(1L, "addr1", "addr2"));
        ClusterStatus cs = new ClusterStatus(42L, true, null, members);
        assertEquals(42L, cs.getSelfId());
        assertTrue(cs.isLeader());
        assertNull(cs.getCurrentLeaderId());
        assertEquals(1, cs.getMembers().size());

        ClusterStatus cs2 = new ClusterStatus(1L, false, 99L, new ArrayList<>());
        assertEquals(Long.valueOf(99L), cs2.getCurrentLeaderId());
    }
}
