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
package com.arcadedb.server.ha;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Binary;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.log.LogManager;
import com.arcadedb.network.binary.ChannelBinaryServer;
import com.arcadedb.network.binary.ConnectionException;
import com.arcadedb.server.ha.message.CommandForwardRequest;
import com.arcadedb.server.ha.message.HACommand;
import com.arcadedb.server.ha.message.ReplicaConnectHotResyncResponse;
import com.arcadedb.server.ha.message.TxForwardRequest;
import com.arcadedb.utility.Callable;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.Pair;
import com.conversantmedia.util.concurrent.PushPullBlockingQueue;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.stream.Collectors;

/**
 * This executor has an intermediate level of buffering managed with a queue. This avoids the Leader to be blocked in case the
 * remote replica does not read messages and the socket remains full causing a block in the sending of messages for all the
 * servers.
 */
public class Leader2ReplicaNetworkExecutor extends Thread {

  public enum STATUS {
    JOINING, OFFLINE, ONLINE
  }

  private final    HAServer                                           server;
  private final    HAServer.ServerInfo                                remoteServer;
  private final    BlockingQueue<Binary>                              senderQueue;
  private          Thread                                             senderThread;
  private final    BlockingQueue<Pair<ReplicationMessage, HACommand>> forwarderQueue;
  private          Thread                                             forwarderThread;
  private          long                                               joinedOn;
  private          long                                               leftOn                = 0;
  private          ChannelBinaryServer                                channel;
  private          STATUS                                             status                = STATUS.JOINING;
  private final    Object                                             lock                  = new Object(); // NOT FINAL BECAUSE IT CAN BE MERGED FROM ANOTHER CONNECTION
  private final    Object                                             channelOutputLock     = new Object();
  private final    Object                                             channelInputLock      = new Object();
  private volatile boolean                                            shutdownCommunication = false;

  // STATS
  private long totalMessages;
  private long totalBytes;
  private long latencyMin;
  private long latencyMax;
  private long latencyTotalTime;

  public Leader2ReplicaNetworkExecutor(final HAServer ha, final ChannelBinaryServer channel, HAServer.ServerInfo remoteServer)
      throws IOException {
    this.server = ha;
    this.remoteServer = remoteServer;
    this.channel = channel;

    final ContextConfiguration cfg = ha.getServer().getConfiguration();
    final int queueSize = cfg.getValueAsInteger(GlobalConfiguration.HA_REPLICATION_QUEUE_SIZE);

    final String cfgQueueImpl = cfg.getValueAsString(GlobalConfiguration.ASYNC_OPERATIONS_QUEUE_IMPL);
    if ("fast".equalsIgnoreCase(cfgQueueImpl)) {
      this.senderQueue = new PushPullBlockingQueue<>(queueSize);
      this.forwarderQueue = new PushPullBlockingQueue<>(queueSize);
    } else if ("standard".equalsIgnoreCase(cfgQueueImpl)) {
      this.senderQueue = new ArrayBlockingQueue<>(queueSize);
      this.forwarderQueue = new ArrayBlockingQueue<>(queueSize);
    } else {
      // WARNING AND THEN USE THE DEFAULT
      LogManager.instance()
          .log(this, Level.WARNING, "Error on async operation queue implementation setting: %s is not supported", null,
              cfgQueueImpl);
      this.senderQueue = new ArrayBlockingQueue<>(queueSize);
      this.forwarderQueue = new ArrayBlockingQueue<>(queueSize);
    }

    setName(server.getServer().getServerName() + " leader2replica->?");

    synchronized (channelOutputLock) {
      try {
        if (!ha.isLeader()) {
          final Replica2LeaderNetworkExecutor leader = server.getLeader();

          this.channel.writeBoolean(false);
          this.channel.writeByte(ReplicationProtocol.ERROR_CONNECT_NOLEADER);
          this.channel.writeString("Current server '" + ha.getServerName() + "' is not the Leader");
          this.channel.writeString(leader != null ? leader.getRemoteServerName() : "");
          this.channel.writeString(leader != null ? leader.getRemoteAddress() : "");
          throw new ConnectionException(channel.socket.getInetAddress().toString(),
              "Current server '" + ha.getServerName() + "' is not the Leader");
        }

        final HAServer.ElectionStatus electionStatus = ha.getElectionStatus();
        if (electionStatus != HAServer.ElectionStatus.DONE
            && electionStatus != HAServer.ElectionStatus.LEADER_WAITING_FOR_QUORUM) {
          this.channel.writeBoolean(false);
          this.channel.writeByte(ReplicationProtocol.ERROR_CONNECT_ELECTION_PENDING);
          this.channel.writeString("Election for the Leader is pending");
          throw new ConnectionException(channel.socket.getInetAddress().toString(), "Election for Leader is pending");
        }

        setName(server.getServer().getServerName() + " leader2replica->" + remoteServer.toString());

        // CONNECTED
        this.channel.writeBoolean(true);

        this.channel.writeString(server.getServerName());
        this.channel.writeLong(server.lastElectionVote != null ? server.lastElectionVote.getFirst() : 1);
        this.channel.writeString(server.getServer().getHttpServer().getListeningAddress());
        this.channel.writeString(
            server.getCluster().getServers().stream().map(HAServer.ServerInfo::toString).collect(Collectors.joining()));

        LogManager.instance()
            .log(this, Level.INFO, "Remote Replica server '%s'  successfully connected", remoteServer);

      } finally {
        this.channel.flush();
      }
    }
  }

  public void mergeFrom(final Leader2ReplicaNetworkExecutor previousConnection) {
    synchronized (previousConnection.lock) {
      senderQueue.addAll(previousConnection.senderQueue);
      previousConnection.close();
    }
  }

  @Override
  public void run() {
    LogManager.instance().setContext(server.getServerName());

    startSenderThread();
    startForwarderThread();

    final Binary buffer = new Binary(8192);

    while (!shutdownCommunication) {
      try {
        handleIncomingRequest(buffer);
      } catch (final TimeoutException e) {
        LogManager.instance().log(this, Level.FINE, "Request in timeout (cause=%s)", e.getCause());
      } catch (final IOException e) {
        handleIOException(e);
      } catch (final Exception e) {
        handleGenericException(e);
      }
    }
  }

  private void startSenderThread() {
    senderThread = new Thread(() -> {
      LogManager.instance().setContext(server.getServerName());
      Binary lastMessage = null;
      while (!shutdownCommunication || !senderQueue.isEmpty()) {
        try {
          lastMessage = processSenderQueue(lastMessage);
        } catch (final IOException | InterruptedException e) {
          handleSenderThreadException(e);
          return;
        }
      }
      LogManager.instance()
          .log(this, Level.FINE, "Replication thread to remote server '%s' is off (buffered=%d)", remoteServer, senderQueue.size());
    });
    senderThread.start();
    senderThread.setName(server.getServer().getServerName() + " leader2replica-sender->" + remoteServer);
  }

  private Binary processSenderQueue(Binary lastMessage) throws IOException, InterruptedException {
    if (lastMessage == null) {
      lastMessage = senderQueue.poll(500, TimeUnit.MILLISECONDS);
    }

    if (lastMessage == null) {
      return null;
    }

    if (shutdownCommunication) {
      return null;
    }

    switch (status) {
    case ONLINE:
      LogManager.instance()
          .log(this, Level.FINE, "Sending message to replica '%s' (msgSize=%d buffered=%d)...", remoteServer, lastMessage.size(),
              senderQueue.size());
      sendMessage(lastMessage);
      return null;
    default:
      LogManager.instance().log(this, Level.FINE, "Replica '%s' is not online, waiting and retry (buffered=%d)...", remoteServer,
          senderQueue.size());
      Thread.sleep(500);
      return lastMessage;
    }
  }

  private void handleSenderThreadException(Exception e) {
    if (e instanceof IOException) {
      LogManager.instance()
          .log(this, Level.INFO, "Error on sending replication message to remote server '%s' (error=%s)", remoteServer, e);
    } else if (e instanceof InterruptedException) {
      Thread.currentThread().interrupt();
    }
    shutdownCommunication = true;
  }

  private void startForwarderThread() {
    forwarderThread = new Thread(() -> {
      LogManager.instance().setContext(server.getServerName());
      final Binary buffer = new Binary(8192);
      buffer.setAllocationChunkSize(1024);

      while (!shutdownCommunication || !forwarderQueue.isEmpty()) {
        try {
          processForwarderQueue(buffer);
        } catch (final IOException | InterruptedException e) {
          handleForwarderThreadException(e);
          return;
        }
      }
      LogManager.instance().log(this, Level.FINE, "Replication thread to remote server '%s' is off (buffered=%d)", remoteServer,
          forwarderQueue.size());
    });
    forwarderThread.start();
    forwarderThread.setName(server.getServer().getServerName() + " leader-forwarder");
  }

  private void processForwarderQueue(Binary buffer) throws IOException, InterruptedException {
    final Pair<ReplicationMessage, HACommand> lastMessage = forwarderQueue.poll(500, TimeUnit.MILLISECONDS);

    if (lastMessage == null) {
      return;
    }

    if (shutdownCommunication) {
      return;
    }

    executeMessage(buffer, lastMessage);
  }

  private void handleForwarderThreadException(Exception e) {
    if (e instanceof IOException) {
      LogManager.instance()
          .log(this, Level.INFO, "Error on sending replication message to remote server '%s' (error=%s)", remoteServer, e);
    } else if (e instanceof InterruptedException) {
      Thread.currentThread().interrupt();
    }
    shutdownCommunication = true;
  }

  private void handleIncomingRequest(Binary buffer) throws IOException, InterruptedException {
    Pair<ReplicationMessage, HACommand> request = server.getMessageFactory().deserializeCommand(buffer, readRequest());

    if (request == null) {
      channel.clearInput();
      return;
    }

    final HACommand command = request.getSecond();

    LogManager.instance()
        .log(this, Level.INFO, "Leader received message %d from replica %s: %s", request.getFirst().messageNumber, remoteServer,
            command);

    if (command instanceof TxForwardRequest || command instanceof CommandForwardRequest) {
      forwarderQueue.put(request);
    } else {
      executeMessage(buffer, request);
    }
  }

  private void handleIOException(IOException e) {
    LogManager.instance().log(this, Level.FINE, "IO Error from reading requests (cause=%s)", e.getCause());
    server.setReplicaStatus(remoteServer, false);
    close();
  }

  private void handleGenericException(Exception e) {
    LogManager.instance().log(this, Level.SEVERE, "Generic error during applying of request from Leader (cause=%s)", e.toString());
    server.setReplicaStatus(remoteServer, false);
    close();
  }

  public int getMessagesInQueue() {
    return senderQueue.size();
  }

  private void executeMessage(final Binary buffer, final Pair<ReplicationMessage, HACommand> request) throws IOException {
    final ReplicationMessage message = request.getFirst();

    final HACommand response = request.getSecond().execute(server, remoteServer, message.messageNumber);

    if (response != null) {
      // SEND THE RESPONSE BACK (USING THE SAME BUFFER)
      server.getMessageFactory().serializeCommand(response, buffer, message.messageNumber);

      LogManager.instance().log(this, Level.FINE, "Request %s -> %s to '%s'", request.getSecond(), response, remoteServer);

      sendMessage(buffer);

      if (response instanceof ReplicaConnectHotResyncResponse resyncResponse) {
        server.resendMessagesToReplica(resyncResponse.getMessageNumber(), remoteServer);
        server.setReplicaStatus(remoteServer, true);
      }
    }
  }

  private byte[] readRequest() throws IOException {
    synchronized (channelInputLock) {
      return channel.readBytes();
    }
  }

  /**
   * Test purpose only.
   */
  public void closeChannel() {
    final ChannelBinaryServer c = channel;
    if (c != null) {
      c.close();
      channel = null;
    }
  }

  public void close() {
    executeInLock((ignore) -> {
      shutdownCommunication = true;

      try {
        final Thread qt = senderThread;
        if (qt != null) {
          try {
            qt.join(1_000);
            senderThread = null;
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            // IGNORE IT
          }
        }

        final Thread ft = forwarderThread;
        if (ft != null) {
          try {
            ft.join(1_000);
            forwarderThread = null;
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            // IGNORE IT
          }
        }

        closeChannel();

      } catch (final Exception e) {
        // IGNORE IT
      }
      return null;
    });
  }

  public boolean enqueueMessage(final long msgNumber, final Binary message) {
    if (status == STATUS.OFFLINE)
      return false;

    return (boolean) executeInLock(new Callable<>() {
      @Override
      public Object call(final Object iArgument) {
        // WRITE DIRECTLY TO THE MESSAGE QUEUE
        if (senderQueue.size() > 1)
          LogManager.instance()
              .log(this, Level.FINE, "Buffering request %d to server '%s' (status=%s buffered=%d)", msgNumber, remoteServer,
                  status, senderQueue.size());

        if (!senderQueue.offer(message)) {
          if (status == STATUS.OFFLINE)
            return false;

          // BACK-PRESSURE
          LogManager.instance()
              .log(this, Level.WARNING, "Applying back-pressure on replicating messages to server '%s' (latency=%s buffered=%d)...",
                  getRemoteServerName(), getLatencyStats(), senderQueue.size());
          try {
            Thread.sleep(1000);
          } catch (final InterruptedException e) {
            // IGNORE IT
            Thread.currentThread().interrupt();
            throw new ReplicationException("Error on replicating to server '" + remoteServer + "'");
          }

          if (status == STATUS.OFFLINE)
            return false;

          if (!senderQueue.offer(message)) {
            LogManager.instance()
                .log(this, Level.INFO, "Timeout on writing request to server '%s', setting it offline...", getRemoteServerName());

//            LogManager.instance().log(this, Level.INFO, "THREAD DUMP:\n%s", FileUtils.threadDump());

            senderQueue.clear();
            server.setReplicaStatus(remoteServer, false);

            // QUEUE FULL, THE REMOTE SERVER COULD BE STUCK SOMEWHERE. REMOVE THE REPLICA
            throw new ReplicationException("Replica '" + remoteServer + "' is not reading replication messages");
          }
        }

        totalBytes += message.size();

        return true;
      }
    });
  }

  public void setStatus(final STATUS status) {
    if (this.status == status)
      // NO STATUS CHANGE
      return;

    executeInLock(new Callable<>() {
      @Override
      public Object call(final Object iArgument) {
        Leader2ReplicaNetworkExecutor.this.status = status;
        LogManager.instance().log(this, Level.INFO, "Replica server '%s' is %s", remoteServer, status);

        Leader2ReplicaNetworkExecutor.this.leftOn = status == STATUS.OFFLINE ? 0 : System.currentTimeMillis();

        if (status == STATUS.ONLINE) {
          Leader2ReplicaNetworkExecutor.this.joinedOn = System.currentTimeMillis();
          Leader2ReplicaNetworkExecutor.this.leftOn = 0;
        } else if (status == STATUS.OFFLINE) {
          Leader2ReplicaNetworkExecutor.this.leftOn = System.currentTimeMillis();
          close();
        }
        return null;
      }
    });

    if (server.getServer().isStarted())
      server.printClusterConfiguration();
  }

  public HAServer.ServerInfo getRemoteServerName() {
    return remoteServer;
  }

  public String getRemoteServerAddress() {
    return remoteServer.toString();
  }

  public long getJoinedOn() {
    return joinedOn;
  }

  public long getLeftOn() {
    return leftOn;
  }

  public void updateStats(final long sentOn, final long receivedOn) {
    totalMessages++;

    final long delta = receivedOn - sentOn;
    latencyTotalTime += delta;

    if (latencyMin == -1 || delta < latencyMin)
      latencyMin = delta;
    if (delta > latencyMax)
      latencyMax = delta;
  }

  public STATUS getStatus() {
    return status;
  }

  public String getLatencyStats() {
    if (totalMessages == 0)
      return "";
    return "avg=" + (latencyTotalTime / totalMessages) + " (min=" + latencyMin + " max=" + latencyMax + ")";
  }

  public String getThroughputStats() {
    if (totalBytes == 0)
      return "";
    return FileUtils.getSizeAsString(totalBytes) + " (" + FileUtils.getSizeAsString(
        (int) (((double) totalBytes / (System.currentTimeMillis() - joinedOn)) * 1000)) + "/s)";
  }

  public void sendMessage(final Binary msg) throws IOException {
    synchronized (channelOutputLock) {
      final ChannelBinaryServer c = channel;
      if (c == null) {
        close();
        throw new IOException("Channel closed");
      }

      c.writeVarLengthBytes(msg.getContent(), msg.size());
      c.flush();
    }
  }

  @Override
  public String toString() {
    return remoteServer.toString();
  }

  // DO I NEED THIS?
  protected Object executeInLock(final Callable<Object, Object> callback) {
    synchronized (lock) {
      return callback.call(null);
    }
  }
}
