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
package com.arcadedb.server.ha.raft;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

/**
 * The one line a cluster gets when {@code arcadedb.ssl.enabled} is set and a peer-to-peer dial still has to go
 * out over the plain listener, because no {@code https} endpoint resolves for the peer it is reaching.
 * <p>
 * Every such dial carries the cluster token - {@code X-ArcadeDB-Cluster-Token} on the probes, an
 * {@code Authorization} header on the shutdown command - so that fallback puts a shared secret on the wire in
 * clear text. Falling back rather than refusing is deliberate and is the package-wide rule: the plain listener
 * is the one that is always bound, and an SSL cluster that never declared the optional 5th field of
 * {@code arcadedb.ha.serverList} would otherwise be unable to bootstrap, resync or be administered at all. But
 * silence is not part of that rule, and it was: an operator who believes the cluster is encrypted end to end
 * has no way to discover the gap from the outside (issues #7546, #7563).
 * <p>
 * <b>One latch for the whole family, not one per dial.</b> The three dials that can fall back say the same
 * thing and are fixed by the same single setting, so three copies of it would only push the first one out of
 * the operator's scrollback. The line names which dial happened to be first; the remedy it names is the same
 * whichever that was.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class PlainHttpFallbackNotice {

  private static final AtomicBoolean WARNED = new AtomicBoolean(false);

  private PlainHttpFallbackNotice() {
  }

  /**
   * Says it, if it has not been said yet in this JVM.
   *
   * @param dial the class of the dial that fell back, so the line is attributed where it happened
   * @param what what that dial was doing, phrased to follow "no HTTPS address is known for a peer; " - e.g.
   *             {@code "probing its bootstrap-state"}
   *
   * @return {@code true} when this call was the one that logged it, {@code false} when it had already been said
   */
  static boolean sayOnce(final Class<?> dial, final String what) {
    if (!WARNED.compareAndSet(false, true))
      return false;
    LogManager.instance().log(dial, Level.WARNING,
        "SSL is enabled but no HTTPS address is known for a peer; %s over plain HTTP, which puts the cluster token "
            + "on the wire in clear text. Declare each node's 'https' port in %s to avoid it. This notice is logged "
            + "once per JVM, and covers every peer-to-peer dial that falls back the same way.",
        what, GlobalConfiguration.HA_SERVER_LIST.getKey());
    return true;
  }

  /** Re-arms the latch. Test-only: a static latch outlives the test that tripped it. */
  // @VisibleForTesting
  static void rearmForTests() {
    WARNED.set(false);
  }
}
