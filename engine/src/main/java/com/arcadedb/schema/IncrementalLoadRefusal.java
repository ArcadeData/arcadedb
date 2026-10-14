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
package com.arcadedb.schema;

/**
 * What {@link LocalSchema#tryLoadIncremental} answers when it declines to refresh the schema incrementally (issue #10020):
 * the {@link FullSchemaLoadReason reason}, and a {@code detail} naming what triggered it, which can be a file id or a file
 * extension and so stays out of the reason (a metric tag).
 *
 * @param reason the closed-set cause
 * @param detail what caused it, for the log line; never {@code null}
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public record IncrementalLoadRefusal(FullSchemaLoadReason reason, String detail) {

  public static IncrementalLoadRefusal of(final FullSchemaLoadReason reason, final String detail) {
    return new IncrementalLoadRefusal(reason, detail == null ? "" : detail);
  }

  @Override
  public String toString() {
    return detail.isEmpty() ? reason.getTag() : reason.getTag() + " (" + detail + ")";
  }
}
