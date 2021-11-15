package com.arcadedb.server.ws;

import com.arcadedb.log.LogManager;
import com.arcadedb.server.BaseGraphServerTest;
import io.undertow.connector.ByteBufferPool;
import io.undertow.server.DefaultByteBufferPool;
import io.undertow.util.StringWriteChannelListener;
import io.undertow.websockets.client.WebSocketClient;
import io.undertow.websockets.client.WebSocketClientNegotiation;
import io.undertow.websockets.core.AbstractReceiveListener;
import io.undertow.websockets.core.BufferedTextMessage;
import io.undertow.websockets.core.CloseMessage;
import io.undertow.websockets.core.WebSocketChannel;
import io.undertow.websockets.core.WebSocketFrameType;
import io.undertow.websockets.core.WebSockets;
import org.junit.jupiter.api.Assertions;
import org.xnio.OptionMap;
import org.xnio.Options;
import org.xnio.Xnio;
import org.xnio.XnioWorker;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;

import static org.apache.lucene.store.BufferedIndexInput.BUFFER_SIZE;

public class WebSocketClientHelper implements AutoCloseable {
  private final XnioWorker                 WORKER;
  private final ByteBufferPool             pool         = new DefaultByteBufferPool(true, BUFFER_SIZE, 1000, 10, 100);
  private final WebSocketChannel           channel;
  private final ArrayBlockingQueue<String> messageQueue = new ArrayBlockingQueue<>(20);

  private static final int DEFAULT_DELAY = 5_000;

  public WebSocketClientHelper(String uri, String user, String pass) throws URISyntaxException, IOException {
    final Xnio xnio = Xnio.getInstance(BaseGraphServerTest.class.getClassLoader());
    WORKER = xnio.createWorker(OptionMap.builder()//
        .set(Options.WORKER_IO_THREADS, 4)//
        .set(Options.CONNECTION_HIGH_WATER, 1000000)//
        .set(Options.CONNECTION_LOW_WATER, 1000000)//
        .set(Options.TCP_NODELAY, true)//
        .set(Options.CORK, true)//
        .getMap());

    var builder = WebSocketClient.connectionBuilder(WORKER, pool, new URI(uri));
    if (user != null) {
      builder.setClientNegotiation(new WebSocketClientNegotiation(new ArrayList<>(), new ArrayList<>()) {
        @Override
        public void beforeRequest(Map<String, List<String>> headers) {
          headers.put("Authorization", Collections.singletonList("Basic " + Base64.getEncoder().encodeToString((user + ":" + pass).getBytes())));
        }
      });
    }
    this.channel = builder.connect().get();
    this.channel.getReceiveSetter().set(new AbstractReceiveListener() {
      @Override
      protected void onFullTextMessage(WebSocketChannel channel, BufferedTextMessage message) {
        messageQueue.offer(message.getData());
      }

      @Override
      protected void onError(WebSocketChannel channel, Throwable error) {
        LogManager.instance().log(this, Level.INFO, "WS client error: " + error);
        super.onError(channel, error);
        Assertions.fail(error.getMessage());
      }
    });
    this.channel.resumeReceives();
  }

  @Override
  public void close() throws IOException {
    LogManager.instance().log(this, Level.INFO, "WS client send close");
    WebSockets.sendCloseBlocking(CloseMessage.NORMAL_CLOSURE, null, this.channel);
    this.channel.flush();
    this.channel.close();
    pool.close();
    messageQueue.clear();
  }

  public void breakConnection() throws IOException {
    LogManager.instance().log(this, Level.INFO, "WS client break connection");
    this.channel.close();
    pool.close();
    messageQueue.clear();
  }

  public String send(String payload) throws URISyntaxException, IOException {
    var sendChannel = this.channel.send(WebSocketFrameType.TEXT);
    new StringWriteChannelListener(payload).setup(sendChannel);
    return this.popMessage(DEFAULT_DELAY);
  }

  public String popMessage() {
    return this.popMessage(DEFAULT_DELAY);
  }

  public String popMessage(int delayMS) {
    try {
      return this.messageQueue.poll(delayMS, TimeUnit.MILLISECONDS);
    } catch (InterruptedException ignored) {
    }

    return null;
  }

}
