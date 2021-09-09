package com.orientechnologies.orient.server.hazelcast;

import com.hazelcast.core.HazelcastInstance;
import com.orientechnologies.common.types.OModifiableInteger;
import com.orientechnologies.orient.server.distributed.ODistributedLockManager;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class OHazelcastLockManager implements ODistributedLockManager {
  private HazelcastInstance hazelcast;
  private Set<String> lockedResurces = new HashSet<>();

  private static final ThreadLocal<Map<String, OModifiableInteger>> ACQUIRED_REENTRANT =
      ThreadLocal.withInitial(
          () -> {
            return new HashMap<>();
          });

  public OHazelcastLockManager(HazelcastInstance hazelcast) {
    this.hazelcast = hazelcast;
  }

  @Override
  public void acquireExclusiveLock(String resource, String nodeSource, long timeout) {
    OModifiableInteger counter;
    if ((counter = ACQUIRED_REENTRANT.get().get(resource)) != null) {
      counter.increment();
      return;
    }

    if (timeout != 0) {
      try {
        hazelcast.getLock(resource).tryLock(timeout, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    } else {
      hazelcast.getLock(resource).lock();
    }
    synchronized (this) {
      lockedResurces.add(resource);
    }
    ACQUIRED_REENTRANT.get().put(resource, new OModifiableInteger(0));
  }

  @Override
  public void releaseExclusiveLock(String resource, String nodeSource) {
    OModifiableInteger counter = ACQUIRED_REENTRANT.get().get(resource);
    if (counter == null)
      // DO NOTHING BECAUSE THE ACQUIRE DIDN'T HAPPEN IN DISTRIBUTED
      return;

    if (counter.getValue() > 0) {
      counter.decrement();
      return;
    }

    ACQUIRED_REENTRANT.get().remove(resource);
    hazelcast.getLock(resource).unlock();
    synchronized (this) {
      lockedResurces.remove(resource);
    }
  }

  @Override
  public void handleUnreachableServer(String nodeLeftName) {}

  @Override
  public void shutdown() {
    synchronized (this) {
      for (String resource : lockedResurces) {
        hazelcast.getLock(resource).unlock();
      }
      lockedResurces.clear();
    }
  }
}
