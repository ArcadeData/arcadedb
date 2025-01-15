/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.server.ws;

import com.arcadedb.log.LogManager;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.utility.CodeUtils;
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

import org.assertj.core.api.Assertions;
import org.xnio.OptionMap;
import org.xnio.Options;
import org.xnio.Xnio;
import org.xnio.XnioWorker;

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.logging.*;

import static org.apache.lucene.store.BufferedIndexInput.BUFFER_SIZE;
import static org.assertj.core.api.Assertions.fail;

public class WebSocketClientHelper implements AutoCloseable {
  private final XnioWorker                 worker;
  private final ByteBufferPool             pool         = new DefaultByteBufferPool(true, BUFFER_SIZE, 1000, 10, 100);
  private final WebSocketChannel           channel;
  private final ArrayBlockingQueue<String> messageQueue = new ArrayBlockingQueue<>(20);

  private static final int DEFAULT_DELAY = 5_000;

  public WebSocketClientHelper(final String uri, final String user, final String pass) throws URISyntaxException, IOException {
    final Xnio xnio = Xnio.getInstance(BaseGraphServerTest.class.getClassLoader());
    worker = xnio.createWorker(OptionMap.builder()//
        .set(Options.WORKER_IO_THREADS, 4)//
        .set(Options.CONNECTION_HIGH_WATER, 1000000)//
        .set(Options.CONNECTION_LOW_WATER, 1000000)//
        .set(Options.TCP_NODELAY, true)//
        .set(Options.CORK, true)//
        .getMap());

    final var builder = WebSocketClient.connectionBuilder(worker, pool, new URI(uri));
    if (user != null) {
      builder.setClientNegotiation(new WebSocketClientNegotiation(new ArrayList<>(), new ArrayList<>()) {
        @Override
        public void beforeRequest(final Map<String, List<String>> headers) {
          headers.put("Authorization", List.of("Basic " + Base64.getEncoder().encodeToString((user + ":" + pass).getBytes())));
        }
      });
    }
    this.channel = builder.connect().get();
    this.channel.getReceiveSetter().set(new AbstractReceiveListener() {
      @Override
      protected void onFullTextMessage(final WebSocketChannel channel, final BufferedTextMessage message) {
        messageQueue.offer(message.getData());
      }

      @Override
      protected void onError(final WebSocketChannel channel, final Throwable error) {
        LogManager.instance().log(this, Level.SEVERE, "WS client error: " + error);
        super.onError(channel, error);
        fail(error.getMessage());
      }
    });
    this.channel.resumeReceives();
  }

  @Override
  public void close() throws IOException {
    LogManager.instance().log(this, Level.FINE, "WS client send close");
    CodeUtils.executeIgnoringExceptions(() -> WebSockets.sendCloseBlocking(CloseMessage.NORMAL_CLOSURE, null, this.channel));
    CodeUtils.executeIgnoringExceptions(this.channel::flush);
    CodeUtils.executeIgnoringExceptions(this.channel::close);
    CodeUtils.executeIgnoringExceptions(pool::close);
    CodeUtils.executeIgnoringExceptions(worker::shutdown);
    messageQueue.clear();
  }

  public void breakConnection() {
    LogManager.instance().log(this, Level.FINE, "WS client break connection");
    CodeUtils.executeIgnoringExceptions(this.channel::close);
    CodeUtils.executeIgnoringExceptions(pool::close);
    messageQueue.clear();
  }

  public String send(final String payload) throws URISyntaxException, IOException {
    final var sendChannel = this.channel.send(WebSocketFrameType.TEXT);
    new StringWriteChannelListener(payload).setup(sendChannel);
    return this.popMessage(DEFAULT_DELAY);
  }

  public String popMessage() {
    return this.popMessage(DEFAULT_DELAY);
  }

  public String popMessage(final int delayMS) {
    try {
      return this.messageQueue.poll(delayMS, TimeUnit.MILLISECONDS);
    } catch (final InterruptedException ignored) {
    }

    return null;
  }

}
