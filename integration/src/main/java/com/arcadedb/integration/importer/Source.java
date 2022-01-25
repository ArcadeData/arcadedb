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
package com.arcadedb.integration.importer;

import com.arcadedb.log.LogManager;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.Callable;
import java.util.logging.Level;

public class Source {
  public final  String                                      url;
  public        InputStream                                 inputStream;
  public final  long                                        totalSize;
  public final  boolean                                     compressed;
  private final com.arcadedb.utility.Callable<Void, Source> resetCallback;
  private final Callable<Void>                              closeCallback;

  public Source(final String url, final InputStream inputStream, final long totalSize, final boolean compressed,
      final com.arcadedb.utility.Callable<Void, Source> resetCallback, final Callable<Void> closeCallback) {
    this.url = url;
    this.inputStream = inputStream;
    this.totalSize = totalSize;
    this.compressed = compressed;
    this.resetCallback = resetCallback;
    this.closeCallback = closeCallback;
  }

  public void reset() throws IOException {
    if (resetCallback != null)
      try {
        resetCallback.call(this);
      } catch (Exception e) {
        LogManager.instance().log(this, Level.SEVERE, "Error on resetting source %s", e, this);
      }
  }

  public void close() {
    try {
      closeCallback.call();
    } catch (Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on closing source %s", e, this);
    }
  }

  @Override
  public String toString() {
    return url + " (compressed=" + compressed + " size=" + totalSize + ")";
  }
}
