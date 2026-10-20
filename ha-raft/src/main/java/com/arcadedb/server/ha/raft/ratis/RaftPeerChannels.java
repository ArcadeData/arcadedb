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
package com.arcadedb.server.ha.raft.ratis;

import org.apache.ratis.thirdparty.io.grpc.Channel;
import org.apache.ratis.thirdparty.io.grpc.ManagedChannel;
import org.apache.ratis.thirdparty.io.grpc.stub.AbstractStub;
import org.apache.ratis.util.MemoizedSupplier;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Lists the gRPC connections Apache Ratis holds for one peer (issue #10374), so that a self-dial probe can be sent over
 * each of them rather than over one.
 * <p>
 * Ratis does not keep "a channel" to a peer. Its {@code GrpcServerProtocolClient} (Ratis 3.3.1) builds a primary channel,
 * which carries {@code requestVote}, {@code startLeaderElection}, the {@code appendLog} stream and {@code installSnapshot};
 * a second, independent one for the heartbeat {@code appendEntries} stream when {@code raft.grpc.server.heartbeat.channel}
 * is on, which is the Ratis default; and a pool of further channels for {@code readIndex} when
 * {@code raft.grpc.server.stub.pool.size} is above one. Each channel resolves the peer's name on its own, at its own time,
 * so one of them can lead back to this node while another reaches the peer. A probe over one channel says nothing about
 * the others.
 * <p>
 * Ratis exposes none of these channels, so they are found by reflection over the proxy's fields. The walk is generic
 * rather than pinned to the three field names: any {@link ManagedChannel} reachable from the proxy through the objects of
 * Ratis's gRPC package, a collection, an array or an initialized {@link MemoizedSupplier} is listed, so a channel added by
 * a Ratis upgrade is probed too. Whatever the walk cannot account for - a stub over a channel it did not find, a map, a
 * plain {@link Channel}, a reflection failure - makes the census {@link Census#complete() incomplete}, and an incomplete
 * census never lets the probe conclude that no channel leads back to this node.
 */
public final class RaftPeerChannels {

  /** Ratis's gRPC package: the objects of it a proxy holds are walked into, anything else is a leaf. */
  private static final String RATIS_GRPC_PACKAGE = "org.apache.ratis.grpc.";
  /** Proxy, pool, pooled stub: deep enough for Ratis 3.3.1 with one level to spare. */
  private static final int    MAX_DEPTH          = 4;

  /**
   * The channels found for a peer, each listed once, and whether they are all the connections the proxy holds for it.
   * A pooled channel that was never built is no connection, so it is not missing from a complete census.
   * {@code unreadable} is why reflection could not read the proxy, or {@code null} when it could: a census can be
   * incomplete with readable fields too, when it finds something it cannot account for.
   */
  public record Census(List<ManagedChannel> channels, boolean complete, String unreadable) {
  }

  private RaftPeerChannels() {
  }

  /** Lists the channels {@code proxy} - a Ratis {@code GrpcServerProtocolClient} - holds for its peer. */
  public static Census of(final Object proxy) {
    if (proxy == null)
      return new Census(List.of(), false, null);
    final Walk walk = new Walk();
    try {
      walk.fieldsOf(proxy, 0);
    } catch (final RuntimeException | IllegalAccessException e) {
      // InaccessibleObjectException in a module-path deployment, missing reflection metadata in a native image
      return new Census(List.copyOf(walk.channels), false, e.toString());
    }
    boolean complete = walk.complete && !walk.channels.isEmpty();
    for (final AbstractStub<?> stub : walk.stubs)
      if (!containsSame(walk.channels, stub.getChannel()))
        complete = false;
    return new Census(List.copyOf(walk.channels), complete, null);
  }

  /** By identity, with a linear scan: a peer has two to a handful of channels. */
  private static boolean containsSame(final List<ManagedChannel> channels, final Channel channel) {
    for (final ManagedChannel c : channels)
      if (c == channel)
        return true;
    return false;
  }

  private static final class Walk {
    private final List<ManagedChannel> channels = new ArrayList<>(2);
    private final List<AbstractStub<?>> stubs    = new ArrayList<>(4);
    // Every object walked into, by identity: a cycle through a collection or an array, which do not count towards the
    // depth, ends here instead of in a StackOverflowError
    private final Set<Object>          visited  = Collections.newSetFromMap(new IdentityHashMap<>());
    private       boolean              complete = true;

    private void fieldsOf(final Object owner, final int depth) throws IllegalAccessException {
      if (depth >= MAX_DEPTH) {
        complete = false;
        return;
      }
      if (!visited.add(owner))
        return;
      for (Class<?> c = owner.getClass(); c != null && c != Object.class; c = c.getSuperclass())
        for (final Field field : c.getDeclaredFields()) {
          if (Modifier.isStatic(field.getModifiers()) || field.getType().isPrimitive())
            continue;
          field.setAccessible(true);
          value(field.get(owner), depth + 1);
        }
    }

    private void value(final Object value, final int depth) throws IllegalAccessException {
      if (value == null)
        return;
      if (value instanceof ManagedChannel channel) {
        if (!containsSame(channels, channel))
          channels.add(channel);
      } else if (value instanceof AbstractStub<?> stub)
        stubs.add(stub);
      else if (value instanceof Channel || value instanceof Map<?, ?>)
        // A channel this walk cannot shut or probe as a connection, or a container it does not look into
        complete = false;
      else if (value instanceof MemoizedSupplier<?> supplier) {
        // A pooled stub Ratis has not built yet holds no connection, and building it here would open one
        if (supplier.isInitialized())
          value(supplier.get(), depth);
      } else if (value instanceof Collection<?> collection) {
        if (visited.add(collection))
          for (final Object element : collection)
            value(element, depth);
      } else if (value.getClass().isArray()) {
        if (!value.getClass().getComponentType().isPrimitive() && visited.add(value))
          for (int i = 0; i < Array.getLength(value); i++)
            value(Array.get(value, i), depth);
      } else if (value.getClass().getName().startsWith(RATIS_GRPC_PACKAGE))
        fieldsOf(value, depth);
    }
  }
}
