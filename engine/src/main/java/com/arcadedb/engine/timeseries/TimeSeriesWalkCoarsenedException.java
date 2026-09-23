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
package com.arcadedb.engine.timeseries;

import com.arcadedb.exception.ArcadeDBException;

/**
 * A row walk was overtaken by a DOWNSAMPLE, so no answer it can still produce is a consistent one (issue #8166).
 * <p>
 * <b>Why this cannot be handed over instead.</b> Every other way a sealed block can leave the directory under a
 * walk is survivable. A compaction or a truncate-driven rewrite copies the retained blocks verbatim, so they keep
 * their {@link TimeSeriesSealedStore.BlockEntry#blockId} and the walk finds them again (issues #7973 and #8043).
 * A retention {@code truncateBefore} drops blocks outright, and a short answer is then the CORRECT answer: the
 * rows really are gone, so the walk counts the block as vanished and carries on.
 * <p>
 * A downsample is neither. It does not remove the rows and it does not retain them - it replaces them with
 * COARSER rows, computed by averaging each (bucket, tag) group, and re-chunks whatever comes out into a fresh set
 * of blocks with no one-to-one relation to the blocks they replaced. So the rows this walk has ALREADY handed to
 * its visitor are fine-grained, and the rows it would hand over from here on are their coarse replacements. There
 * is no way to join the two into one answer: the bucket the walk is standing in would be counted once at each
 * resolution, and a bucket that straddles the crossing point would be counted at neither. Carrying a replaced
 * block's id onto the coarse block that covers it - the shape the issue proposed - does not change that, it only
 * hides it.
 * <p>
 * So the walk stops and says so. A caller that can re-run - {@code EXPORT DATABASE}, a PromQL range read, a
 * Grafana panel - gets a whole answer at one resolution by asking again, and downsampling a series is a
 * maintenance event, not a per-request one, so asking again succeeds. The alternative the fix replaces is the one
 * issue #8043 called the worst available outcome: a silently short answer, with no exception, no log line and -
 * for a caller passing no {@link AggregationMetrics}, which {@code EXPORT DATABASE} does - no count either.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/8166">issue #8166</a>
 */
public class TimeSeriesWalkCoarsenedException extends ArcadeDBException {

  public TimeSeriesWalkCoarsenedException(final String message) {
    super(message);
  }
}
