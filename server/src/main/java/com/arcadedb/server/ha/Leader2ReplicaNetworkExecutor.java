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

import com.arcadedb.Constants;
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
  private final    String                                             remoteServerName;
  private final    String                                             remoteServerAddress;
  private final    String                                             remoteServerHTTPAddress;
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

  public Leader2ReplicaNetworkExecutor(final HAServer ha, final ChannelBinaryServer channel, final String remoteServerName, final String remoteServerAddress,
      final String remoteServerHTTPAddress) throws IOException {
    this.server = ha;
    this.remoteServerName = remoteServerName;
    this.remoteServerAddress = remoteServerAddress;
    this.remoteServerHTTPAddress = remoteServerHTTPAddress;
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
      LogManager.instance().log(this, Level.WARNING, "Error on async operation queue implementation setting: %s is not supported", null, cfgQueueImpl);
      this.senderQueue = new ArrayBlockingQueue<>(queueSize);
      this.forwarderQueue = new ArrayBlockingQueue<>(queueSize);
    }

    setName(Constants.PRODUCT + "-ha-leader2replica/" + server.getServer().getServerName() + "/?");

    synchronized (channelOutputLock) {
      try {
        if (!ha.isLeader()) {
          this.channel.writeBoolean(false);
          this.channel.writeByte(ReplicationProtocol.ERROR_CONNECT_NOLEADER);
          this.channel.writeString("Current server '" + ha.getServerName() + "' is not the Leader");
          this.channel.writeString(server.getLeader().getRemoteServerName());
          this.channel.writeString(server.getLeader().getRemoteAddress());
          throw new ConnectionException(channel.socket.getInetAddress().toString(), "Current server '" + ha.getServerName() + "' is not the Leader");
        }

        final HAServer.ELECTION_STATUS electionStatus = ha.getElectionStatus();
        if (electionStatus != HAServer.ELECTION_STATUS.DONE && electionStatus != HAServer.ELECTION_STATUS.LEADER_WAITING_FOR_QUORUM) {
          this.channel.writeBoolean(false);
          this.channel.writeByte(ReplicationProtocol.ERROR_CONNECT_ELECTION_PENDING);
          this.channel.writeString("Election for the Leader is pending");
          throw new ConnectionException(channel.socket.getInetAddress().toString(), "Election for Leader is pending");
        }

        setName(Constants.PRODUCT + "-ha-leader2replica/" + server.getServer().getServerName() + "/" + remoteServerName + "(" + remoteServerAddress + ")");

        // CONNECTED
        this.channel.writeBoolean(true);

        this.channel.writeString(server.getServerName());
        this.channel.writeLong(server.lastElectionVote != null ? server.lastElectionVote.getFirst() : 1);
        this.channel.writeString(server.getServer().getHttpServer().getListeningAddress());
        this.channel.writeString(this.server.getServerAddressList());

        LogManager.instance().log(this, Level.INFO, "Remote Replica server '%s' (%s) successfully connected", remoteServerName, remoteServerAddress);

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

    senderThread = new Thread(new Runnable() {
      @Override
      public void run() {
        LogManager.instance().setContext(server.getServerName());
        Binary lastMessage = null;
        while (!shutdownCommunication || !senderQueue.isEmpty()) {
          try {
            if (lastMessage == null)
              lastMessage = senderQueue.poll(500, TimeUnit.MILLISECONDS);

            if (lastMessage == null)
              continue;

            if (shutdownCommunication)
              break;

            switch (status) {
            case ONLINE:
              LogManager.instance().log(this, Level.FINE, "Sending message to replica '%s' (msgSize=%d buffered=%d)...", remoteServerName, lastMessage.size(),
                  senderQueue.size());

              sendMessage(lastMessage);
              lastMessage = null;
              break;

            default:
              LogManager.instance().log(this, Level.FINE, "Replica '%s' is not online, waiting and retry (buffered=%d)...", remoteServerName, senderQueue.size());
              Thread.sleep(500);
            }

          } catch (IOException e) {
            LogManager.instance().log(this, Level.INFO, "Error on sending replication message to remote server '%s' (error=%s)", remoteServerName, e);
            shutdownCommunication = true;
            return;
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            break;
          }
        }

        LogManager.instance().log(this, Level.FINE, "Replication thread to remote server '%s' is off (buffered=%d)", remoteServerName, senderQueue.size());

      }
    });
    senderThread.start();
    senderThread.setName(Constants.PRODUCT + "-ha-leader2replica-sender/" + server.getServer().getServerName() + "/" + remoteServerName);

    forwarderThread = new Thread(new Runnable() {
      @Override
      public void run() {
        LogManager.instance().setContext(server.getServerName());

        final Binary buffer = new Binary(8192);
        buffer.setAllocationChunkSize(1024);

        while (!shutdownCommunication || !forwarderQueue.isEmpty()) {
          try {
            final Pair<ReplicationMessage, HACommand> lastMessage = forwarderQueue.poll(500, TimeUnit.MILLISECONDS);

            if (lastMessage == null)
              continue;

            if (shutdownCommunication)
              break;

            executeMessage(buffer, lastMessage);

          } catch (IOException e) {
            LogManager.instance().log(this, Level.INFO, "Error on sending replication message to remote server '%s' (error=%s)", remoteServerName, e);
            shutdownCommunication = true;
            return;
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            break;
          }
        }

        LogManager.instance().log(this, Level.FINE, "Replication thread to remote server '%s' is off (buffered=%d)", remoteServerName, forwarderQueue.size());

      }
    });
    forwarderThread.start();
    forwarderThread.setName(Constants.PRODUCT + "-ha-leader-forwarder/" + server.getServer().getServerName());

    // REUSE THE SAME BUFFER TO AVOID MALLOC
    final Binary buffer = new Binary(8192);

    while (!shutdownCommunication) {
      Pair<ReplicationMessage, HACommand> request = null;
      try {
        request = server.getMessageFactory().deserializeCommand(buffer, readRequest());

        if (request == null) {
          channel.clearInput();
          continue;
        }

        final HACommand command = request.getSecond();

        LogManager.instance().log(this, Level.FINE, "Leader received message %d from replica %s: %s", request.getFirst().messageNumber, remoteServerName, command);

        if (command instanceof TxForwardRequest || command instanceof CommandForwardRequest)
          // EXECUTE IT AS ASYNC
          forwarderQueue.put(request);
        else
          executeMessage(buffer, request);

      } catch (TimeoutException e) {
        LogManager.instance().log(this, Level.FINE, "Request %s in timeout (cause=%s)", request, e.getCause());
      } catch (IOException e) {
        LogManager.instance().log(this, Level.FINE, "IO Error from reading requests (cause=%s)", e.getCause());
        server.setReplicaStatus(remoteServerName, false);
        close();
      } catch (Exception e) {
        LogManager.instance().log(this, Level.SEVERE, "Generic error during applying of request from Leader (cause=%s)", e.toString());
        server.setReplicaStatus(remoteServerName, false);
        close();
      }
    }
  }

  private void executeMessage(final Binary buffer, final Pair<ReplicationMessage, HACommand> request) throws IOException {
    final ReplicationMessage message = request.getFirst();

    final HACommand response = request.getSecond().execute(server, remoteServerName, message.messageNumber);

    if (response != null) {
      // SEND THE RESPONSE BACK (USING THE SAME BUFFER)
      server.getMessageFactory().serializeCommand(response, buffer, message.messageNumber);

      LogManager.instance().log(this, Level.FINE, "Request %s -> %s to '%s'", request.getSecond(), response, remoteServerName);

      sendMessage(buffer);

      if (response instanceof ReplicaConnectHotResyncResponse) {
        server.resendMessagesToReplica(((ReplicaConnectHotResyncResponse) response).getMessageNumber(), remoteServerName);
        server.setReplicaStatus(remoteServerName, true);
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
            qt.join(5000);
            senderThread = null;
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // IGNORE IT
          }
        }

        final Thread ft = forwarderThread;
        if (ft != null) {
          try {
            ft.join(5000);
            forwarderThread = null;
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // IGNORE IT
          }
        }

        closeChannel();

      } catch (Exception e) {
        // IGNORE IT
      }
      return null;
    });
  }

  public boolean enqueueMessage(final Binary message) {
    if (status == STATUS.OFFLINE)
      return false;

    return (boolean) executeInLock(new Callable<>() {
      @Override
      public Object call(Object iArgument) {
        // WRITE DIRECTLY TO THE MESSAGE QUEUE
        if (senderQueue.size() > 1)
          LogManager.instance().log(this, Level.FINE, "Buffering request to server '%s' (status=%s buffered=%d)", remoteServerName, status, senderQueue.size());

        if (!senderQueue.offer(message)) {
          if (status == STATUS.OFFLINE)
            return false;

          // BACK-PRESSURE
          LogManager.instance().log(this, Level.WARNING, "Applying back-pressure on replicating messages to server '%s' (latency=%s buffered=%d)...", getRemoteServerName(),
                  getLatencyStats(), senderQueue.size());
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            // IGNORE IT
            Thread.currentThread().interrupt();
            throw new ReplicationException("Error on replicating to server '" + remoteServerName + "'");
          }

          if (status == STATUS.OFFLINE)
            return false;

          if (!senderQueue.offer(message)) {
            LogManager.instance().log(this, Level.INFO, "Timeout on writing request to server '%s', setting it offline...", getRemoteServerName());

//            LogManager.instance().log(this, Level.INFO, "THREAD DUMP:\n%s", FileUtils.threadDump());

            senderQueue.clear();
            server.setReplicaStatus(remoteServerName, false);

            // QUEUE FULL, THE REMOTE SERVER COULD BE STUCK SOMEWHERE. REMOVE THE REPLICA
            throw new ReplicationException("Replica '" + remoteServerName + "' is not reading replication messages");
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
      public Object call(Object iArgument) {
        Leader2ReplicaNetworkExecutor.this.status = status;
        LogManager.instance().log(this, Level.INFO, "Replica server '%s' is %s", remoteServerName, status);

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

  public String getRemoteServerName() {
    return remoteServerName;
  }

  public String getRemoteServerAddress() {
    return remoteServerAddress;
  }

  public String getRemoteServerHTTPAddress() {
    return remoteServerHTTPAddress;
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
    return remoteServerName;
  }

  // DO I NEED THIS?
  protected Object executeInLock(final Callable<Object, Object> callback) {
    synchronized (lock) {
      return callback.call(null);
    }
  }
}
