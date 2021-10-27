package com.arcadedb.server.ws;

import com.arcadedb.server.BaseGraphServerTest;
import io.undertow.connector.ByteBufferPool;
import io.undertow.server.DefaultByteBufferPool;
import io.undertow.util.StringWriteChannelListener;
import io.undertow.websockets.client.WebSocketClient;
import io.undertow.websockets.client.WebSocketClientNegotiation;
import io.undertow.websockets.core.AbstractReceiveListener;
import io.undertow.websockets.core.BufferedTextMessage;
import io.undertow.websockets.core.WebSocketChannel;
import io.undertow.websockets.core.WebSocketFrameType;
import org.xnio.OptionMap;
import org.xnio.Options;
import org.xnio.Xnio;
import org.xnio.XnioWorker;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.apache.lucene.store.BufferedIndexInput.BUFFER_SIZE;

public class WebSocketClientHelper {
  private static       XnioWorker                 worker;
  private final        WebSocketChannel           channel;
  private static final ByteBufferPool             pool         = new DefaultByteBufferPool(true, BUFFER_SIZE, 1000, 10, 100);
  private final        ArrayBlockingQueue<String> messageQueue = new ArrayBlockingQueue<>(10);

  static {
    Xnio xnio = Xnio.getInstance(BaseGraphServerTest.class.getClassLoader());
    try {
      worker = xnio.createWorker(OptionMap.builder()
          .set(Options.WORKER_IO_THREADS, 2)
          .set(Options.CONNECTION_HIGH_WATER, 1000000)
          .set(Options.CONNECTION_LOW_WATER, 1000000)
          .set(Options.WORKER_TASK_CORE_THREADS, 30)
          .set(Options.WORKER_TASK_MAX_THREADS, 30)
          .set(Options.TCP_NODELAY, true)
          .set(Options.CORK, true)
          .getMap());
    } catch (IOException ignored) {
    }
  }

  public WebSocketClientHelper(String uri, String user, String pass) throws URISyntaxException, IOException {
    var builder = WebSocketClient.connectionBuilder(worker, pool, new URI(uri));
    if (user != null) {
      builder.setClientNegotiation(new WebSocketClientNegotiation(new ArrayList<>(), new ArrayList<>()) {
        @Override
        public void beforeRequest(Map<String, List<String>> headers) {
          headers.put("Authorization",
              Collections.singletonList("Basic " + Base64.getEncoder().encodeToString((user + ":" + pass).getBytes())));
        }
      });
    }
    this.channel = builder.connect().get();

    channel.getReceiveSetter().set(new AbstractReceiveListener() {
      @Override
      protected void onFullTextMessage(WebSocketChannel channel, BufferedTextMessage message) {
        messageQueue.offer(message.getData());
      }

      @Override
      protected void onError(WebSocketChannel channel, Throwable error) {
        super.onError(channel, error);
        messageQueue.offer(error.getMessage());
      }
    });
    channel.resumeReceives();
  }

  public String send(String payload) throws URISyntaxException, IOException {
    var sendChannel = channel.send(WebSocketFrameType.TEXT);
    new StringWriteChannelListener(payload).setup(sendChannel);
    return this.popMessage();
  }

  public String popMessage() {
    try {
      return this.messageQueue.poll(100, TimeUnit.MILLISECONDS);
    } catch (InterruptedException ignored) {
    }

    return null;
  }

  public void close() throws IOException {
    this.channel.sendClose();
  }
}
