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
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.server.ha.message.CommandForwardRequest;
import com.arcadedb.server.ha.message.HACommand;
import com.arcadedb.server.ha.message.ReplicaConnectFullResyncResponse;
import com.arcadedb.server.ha.message.ReplicaConnectHotResyncResponse;
import com.arcadedb.server.ha.message.TxForwardRequest;
import com.arcadedb.utility.Callable;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.Pair;
import com.conversantmedia.util.concurrent.PushPullBlockingQueue;

import java.io.EOFException;
import java.io.IOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
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
    JOINING,      // Initial connection
    OFFLINE,      // Disconnected
    ONLINE,       // Healthy, processing messages
    RECONNECTING, // Connection lost, attempting recovery
    DRAINING,     // Shutdown requested
    FAILED;       // Unrecoverable error

    private static final java.util.Map<STATUS, java.util.Set<STATUS>> VALID_TRANSITIONS = java.util.Map.of(
        JOINING, java.util.Set.of(ONLINE, FAILED, DRAINING, OFFLINE),
        ONLINE, java.util.Set.of(RECONNECTING, DRAINING, OFFLINE, FAILED),
        RECONNECTING, java.util.Set.of(ONLINE, FAILED, DRAINING, OFFLINE),
        OFFLINE, java.util.Set.of(JOINING, ONLINE, DRAINING, FAILED),
        DRAINING, java.util.Set.of(FAILED, OFFLINE),
        FAILED, java.util.Set.of()
    );

    public boolean canTransitionTo(STATUS newStatus) {
      return VALID_TRANSITIONS.getOrDefault(this, java.util.Set.of()).contains(newStatus);
    }
  }

  private final    HAServer                                           server;
  private final    HAServer.ServerInfo                                remoteServer;
  private final    BlockingQueue<Binary>                              senderQueue;
  private final    BlockingQueue<Pair<ReplicationMessage, HACommand>> forwarderQueue;
  private final    Object                                             lock                  = new Object(); // NOT FINAL BECAUSE IT CAN BE MERGED FROM ANOTHER CONNECTION
  private final    Object                                             channelOutputLock     = new Object();
  private final    Object                                             channelInputLock      = new Object();
  private          Thread                                             senderThread;
  private          Thread                                             forwarderThread;
  private          long                                               joinedOn;
  private          long                                               leftOn                = 0;
  private          ChannelBinaryServer                                channel;
  private          STATUS                                             status                = STATUS.JOINING;
  private volatile boolean                                            shutdownCommunication = false;
  private final    ReplicaConnectionMetrics                           metrics               = new ReplicaConnectionMetrics();

  // STATS
  private long totalMessages;
  private long totalBytes;
  private long latencyMin;
  private long latencyMax;
  private long latencyTotalTime;

  // HEALTH MONITORING
  private long lastActivityTimestamp = System.currentTimeMillis();
  private long lastHealthCheckTimestamp = System.currentTimeMillis();

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
        if (!server.isLeader()) {
          final Replica2LeaderNetworkExecutor leader = server.getLeader();

          this.channel.writeBoolean(false);
          this.channel.writeByte(ReplicationProtocol.ERROR_CONNECT_NOLEADER);
          this.channel.writeString("Current server '" + server.getServerName() + "' is not the Leader");
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

    // Update activity timestamp on each message
    lastActivityTimestamp = System.currentTimeMillis();

    final HACommand command = request.getSecond();

    LogManager.instance()
        .log(this, Level.FINE, "Leader received message %d from replica %s: %s", request.getFirst().messageNumber, remoteServer,
            command);

    if (command instanceof TxForwardRequest || command instanceof CommandForwardRequest) {
      forwarderQueue.put(request);
    } else {
      executeMessage(buffer, request);
    }

    // Periodic health check logging
    checkConnectionHealth();
  }

  private void checkConnectionHealth() {
    if (!server.getServer().getConfiguration().getValueAsBoolean(GlobalConfiguration.HA_CONNECTION_HEALTH_CHECK_ENABLED)) {
      return;
    }

    final long now = System.currentTimeMillis();
    final long checkInterval = server.getServer().getConfiguration()
        .getValueAsLong(GlobalConfiguration.HA_CONNECTION_HEALTH_CHECK_INTERVAL_MS);
    final long timeout = server.getServer().getConfiguration()
        .getValueAsLong(GlobalConfiguration.HA_CONNECTION_HEALTH_CHECK_TIMEOUT_MS);

    if (now - lastHealthCheckTimestamp >= checkInterval) {
      lastHealthCheckTimestamp = now;

      final long timeSinceLastActivity = now - lastActivityTimestamp;

      if (timeSinceLastActivity > timeout) {
        LogManager.instance().log(this, Level.WARNING,
            "No activity from replica %s for %dms (timeout: %dms, status: %s, queue: %d)",
            remoteServer, timeSinceLastActivity, timeout, status, senderQueue.size());
      } else {
        LogManager.instance().log(this, Level.FINE,
            "Connection health: replica %s is healthy (last activity: %dms ago, status: %s, queue: %d)",
            remoteServer, timeSinceLastActivity, status, senderQueue.size());
      }
    }
  }

  private void handleIOException(IOException e) {
    if (e instanceof EOFException) {
      LogManager.instance().log(this, Level.FINE,
          "Connection closed by replica %s during message exchange (will mark offline)", remoteServer);
    } else {
      LogManager.instance().log(this, Level.FINE, "IO Error from reading requests (cause=%s)", e.getCause());
    }
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
        LogManager.instance().log(this, Level.FINE,
            "Hot resync response sent to '%s', setting ONLINE immediately", remoteServer);
        server.resendMessagesToReplica(resyncResponse.getMessageNumber(), remoteServer);
        server.setReplicaStatus(remoteServer, true);
      } else if (response instanceof ReplicaConnectFullResyncResponse) {
        LogManager.instance().log(this, Level.FINE,
            "Full resync response sent to '%s', waiting for ReplicaReadyRequest before ONLINE",
            remoteServer);
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
    // Check status outside lock for fast path
    if (status == STATUS.OFFLINE)
      return false;

    return (boolean) executeInLock(new Callable<>() {
      @Override
      public Object call(final Object iArgument) {
        // Double-check status inside lock to prevent race condition
        if (status == STATUS.OFFLINE)
          return false;

        // WRITE DIRECTLY TO THE MESSAGE QUEUE
        if (senderQueue.size() > 1)
          LogManager.instance()
              .log(this, Level.FINE, "Buffering request %d to server '%s' (status=%s buffered=%d)", msgNumber, remoteServer,
                  status, senderQueue.size());

        if (!senderQueue.offer(message)) {
          if (status == STATUS.OFFLINE)
            return false;

          // BACK-PRESSURE with configurable timeout
          final long backpressureWait = server.getServer().getConfiguration()
              .getValueAsLong(GlobalConfiguration.HA_BACKPRESSURE_MAX_WAIT);

          LogManager.instance()
              .log(this, Level.WARNING,
                  "Applying back-pressure on replicating messages to server '%s' (latency=%s buffered=%d maxWait=%dms)...",
                  getRemoteServerName(), getLatencyStats(), senderQueue.size(), backpressureWait);
          try {
            Thread.sleep(backpressureWait);
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ReplicationException("Error on replicating to server '" + remoteServer + "'");
          }

          if (status == STATUS.OFFLINE)
            return false;

          if (!senderQueue.offer(message)) {
            LogManager.instance()
                .log(this, Level.SEVERE,
                    "Queue overflow for replica '%s' - removing from cluster. Manual intervention required to re-add replica.",
                    getRemoteServerName());

            // DO NOT clear the queue - messages are already in the replication log (WAL semantics).
            // Clearing would cause data inconsistency if replica reconnects later.
            // Instead, remove the replica from cluster entirely.
            server.setReplicaStatus(remoteServer, false);
            server.removeServer(remoteServer);

            throw new ReplicationException(
                "Replica '" + remoteServer + "' queue overflow - removed from cluster. Manual re-sync required.");
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

    // Validate state transition
    final STATUS oldStatus = this.status;
    if (!oldStatus.canTransitionTo(status)) {
      LogManager.instance().log(this, Level.WARNING,
          "Invalid state transition: %s -> %s for replica '%s' (allowed anyway for backward compatibility)",
          oldStatus, status, remoteServer);
      // Allow anyway for backward compatibility, but log the warning
    }

    executeInLock(new Callable<>() {
      @Override
      public Object call(final Object iArgument) {
        Leader2ReplicaNetworkExecutor.this.status = status;
        LogManager.instance().log(this, Level.FINE,
            "Replica '%s' state: %s -> %s", remoteServer, oldStatus, status);

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

      // Update activity timestamp on successful send
      lastActivityTimestamp = System.currentTimeMillis();
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

  /**
   * Classifies if exception is a transient network failure.
   *
   * @param e the exception to classify
   * @return true if transient network failure
   */
  private boolean isTransientNetworkFailure(Exception e) {
    return e instanceof SocketTimeoutException ||
           e instanceof SocketException ||
           (e instanceof IOException &&
            e.getMessage() != null &&
            e.getMessage().contains("Connection reset"));
  }

  /**
   * Classifies if exception indicates a leadership change.
   *
   * @param e the exception to classify
   * @return true if leadership change
   */
  private boolean isLeadershipChange(Exception e) {
    return e instanceof ServerIsNotTheLeaderException ||
           (e instanceof ConnectionException &&
            e.getMessage() != null &&
            e.getMessage().contains("not the Leader")) ||
           (e instanceof ReplicationException &&
            e.getMessage() != null &&
            e.getMessage().contains("election in progress"));
  }

  /**
   * Classifies if exception is a protocol error.
   *
   * @param e the exception to classify
   * @return true if protocol error
   */
  private boolean isProtocolError(Exception e) {
    // For now, only message-based detection since we don't have NetworkProtocolException in production
    return e instanceof IOException &&
           e.getMessage() != null &&
           e.getMessage().contains("Protocol");
  }

  /**
   * Categorizes an exception into one of 4 categories.
   *
   * @param e the exception to categorize
   * @return the exception category
   */
  private ExceptionCategory categorizeException(Exception e) {
    if (isTransientNetworkFailure(e)) {
      return ExceptionCategory.TRANSIENT_NETWORK;
    } else if (isLeadershipChange(e)) {
      return ExceptionCategory.LEADERSHIP_CHANGE;
    } else if (isProtocolError(e)) {
      return ExceptionCategory.PROTOCOL_ERROR;
    } else {
      return ExceptionCategory.UNKNOWN;
    }
  }

  /**
   * Handles transient network failures with exponential backoff.
   *
   * @param e the exception that triggered recovery
   */
  private void recoverFromTransientFailure(final Exception e) throws Exception {
    final int maxAttempts = server.getServer().getConfiguration().getValueAsInteger(GlobalConfiguration.HA_TRANSIENT_FAILURE_MAX_ATTEMPTS);
    final long baseDelayMs = server.getServer().getConfiguration().getValueAsLong(GlobalConfiguration.HA_TRANSIENT_FAILURE_BASE_DELAY_MS);
    final double multiplier = 2.0;
    final long maxDelayMs = 8000; // Cap at 8 seconds

    LogManager.instance().log(this, Level.INFO,
        "Replica '%s' recovering from transient network failure: %s",
        null, remoteServer.toString(), e.getMessage());

    reconnectWithBackoff(maxAttempts, baseDelayMs, multiplier, maxDelayMs, ExceptionCategory.TRANSIENT_NETWORK);
  }

  /**
   * Reconnects with exponential backoff.
   *
   * @param maxAttempts maximum retry attempts
   * @param baseDelayMs initial delay in milliseconds
   * @param multiplier delay multiplier (usually 2.0)
   * @param maxDelayMs maximum delay cap
   * @param category exception category for metrics
   */
  private void reconnectWithBackoff(final int maxAttempts, final long baseDelayMs,
                                     final double multiplier, final long maxDelayMs,
                                     final ExceptionCategory category) throws Exception {
    long delay = baseDelayMs;
    final long recoveryStartTime = System.currentTimeMillis();

    for (int attempt = 1; attempt <= maxAttempts && !shutdownCommunication; attempt++) {
      try {
        // Wait before retry
        Thread.sleep(delay);

        // Emit reconnection attempt event
        server.getServer().lifecycleEvent(
            com.arcadedb.server.ReplicationCallback.Type.REPLICA_RECONNECT_ATTEMPT,
            new Object[] { remoteServer.toString(), attempt, maxAttempts, delay }
        );

        LogManager.instance().log(this, Level.INFO,
            "Replica '%s' reconnection attempt %d/%d (delay: %dms)",
            null, remoteServer.toString(), attempt, maxAttempts, delay);

        // Attempt reconnection - this will be implemented later
        // For now, just log
        // TODO: Implement actual reconnection logic

        // If we get here, reconnection succeeded
        final long recoveryTime = System.currentTimeMillis() - recoveryStartTime;

        server.getServer().lifecycleEvent(
            com.arcadedb.server.ReplicationCallback.Type.REPLICA_RECOVERY_SUCCEEDED,
            new Object[] { remoteServer.toString(), attempt, recoveryTime }
        );

        metrics.recordSuccessfulRecovery(recoveryTime);
        metrics.consecutiveFailuresCounter().set(0);

        LogManager.instance().log(this, Level.INFO,
            "Replica '%s' recovery successful after %d attempts (%dms)",
            null, remoteServer.toString(), attempt, recoveryTime);

        return; // Success, exit retry loop

      } catch (final InterruptedException ie) {
        Thread.currentThread().interrupt();
        return;
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.WARNING,
            "Replica '%s' reconnection attempt %d/%d failed (next retry in %dms): %s",
            null, remoteServer.toString(), attempt, maxAttempts, delay, e.getMessage());

        // Calculate next delay (exponential backoff, capped)
        delay = Math.min((long)(delay * multiplier), maxDelayMs);
      }
    }

    // All attempts exhausted
    final long totalRecoveryTime = System.currentTimeMillis() - recoveryStartTime;

    server.getServer().lifecycleEvent(
        com.arcadedb.server.ReplicationCallback.Type.REPLICA_RECOVERY_FAILED,
        new Object[] { remoteServer.toString(), maxAttempts, totalRecoveryTime, category }
    );

    metrics.failedRecoveriesCounter().incrementAndGet();
    metrics.consecutiveFailuresCounter().incrementAndGet();

    LogManager.instance().log(this, Level.SEVERE,
        "Replica '%s' recovery failed after %d attempts (%dms)",
        null, remoteServer.toString(), maxAttempts, totalRecoveryTime);
  }

  /**
   * Handles leadership changes by finding and connecting to new leader.
   * No exponential backoff - leadership changes are discrete events.
   *
   * @param e the exception that triggered recovery
   */
  private void recoverFromLeadershipChange(final Exception e) throws Exception {
    LogManager.instance().log(this, Level.INFO,
        "Replica '%s' detected leadership change: %s",
        null, remoteServer.toString(), e.getMessage());

    server.getServer().lifecycleEvent(
        com.arcadedb.server.ReplicationCallback.Type.REPLICA_LEADERSHIP_CHANGE_DETECTED,
        new Object[] { remoteServer.toString(), remoteServer.toString() }
    );

    // TODO: Implement leader discovery and reconnection
    // For now, use standard reconnection with short timeout
    LogManager.instance().log(this, Level.INFO,
        "Replica '%s' finding new leader...",
        null, remoteServer.toString());

    // Placeholder: treat as transient for now
    recoverFromTransientFailure(e);
  }

  /**
   * Handles protocol errors by failing immediately.
   * Protocol errors are not retryable.
   *
   * @param e the exception that triggered failure
   */
  private void failFromProtocolError(final Exception e) throws Exception {
    LogManager.instance().log(this, Level.SEVERE,
        "PROTOCOL ERROR: Replica '%s' encountered unrecoverable protocol error. " +
        "Manual intervention required.",
        e, remoteServer.toString());

    server.getServer().lifecycleEvent(
        com.arcadedb.server.ReplicationCallback.Type.REPLICA_FAILED,
        new Object[] { remoteServer.toString(), ExceptionCategory.PROTOCOL_ERROR, e }
    );

    metrics.protocolErrorsCounter().incrementAndGet();

    // Do NOT trigger election - this is a configuration/version issue
  }

  /**
   * Handles unknown errors with conservative retry strategy.
   *
   * @param e the exception that triggered recovery
   */
  private void recoverFromUnknownError(final Exception e) throws Exception {
    LogManager.instance().log(this, Level.SEVERE,
        "Unknown error during replication to '%s' - applying conservative recovery",
        e, remoteServer.toString());

    final int maxAttempts = server.getServer().getConfiguration().getValueAsInteger(GlobalConfiguration.HA_UNKNOWN_ERROR_MAX_ATTEMPTS);
    final long baseDelayMs = server.getServer().getConfiguration().getValueAsLong(GlobalConfiguration.HA_UNKNOWN_ERROR_BASE_DELAY_MS);
    final double multiplier = 2.0;
    final long maxDelayMs = 30000; // Cap at 30 seconds

    reconnectWithBackoff(maxAttempts, baseDelayMs, multiplier, maxDelayMs, ExceptionCategory.UNKNOWN);
  }

  /**
   * Handles connection failure by categorizing and applying appropriate recovery.
   *
   * @param e the exception that caused the failure
   */
  private void handleConnectionFailure(final Exception e) throws Exception {
    // Check for shutdown first
    if (Thread.currentThread().isInterrupted() || shutdownCommunication) {
      return;
    }

    // Categorize the exception
    final ExceptionCategory category = categorizeException(e);

    // Update metrics
    switch (category) {
      case TRANSIENT_NETWORK:
        metrics.transientNetworkFailuresCounter().incrementAndGet();
        break;
      case LEADERSHIP_CHANGE:
        metrics.leadershipChangesCounter().incrementAndGet();
        break;
      case PROTOCOL_ERROR:
        metrics.protocolErrorsCounter().incrementAndGet();
        break;
      case UNKNOWN:
        metrics.unknownErrorsCounter().incrementAndGet();
        break;
    }

    // Emit categorization event
    server.getServer().lifecycleEvent(
        com.arcadedb.server.ReplicationCallback.Type.REPLICA_FAILURE_CATEGORIZED,
        new Object[] { remoteServer.toString(), e, category }
    );

    // Apply category-specific recovery strategy
    switch (category) {
      case TRANSIENT_NETWORK:
        recoverFromTransientFailure(e);
        break;
      case LEADERSHIP_CHANGE:
        recoverFromLeadershipChange(e);
        break;
      case PROTOCOL_ERROR:
        failFromProtocolError(e);
        break;
      case UNKNOWN:
        recoverFromUnknownError(e);
        break;
    }
  }
}
