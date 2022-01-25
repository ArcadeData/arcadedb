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
package com.arcadedb.server.ha.message;

import com.arcadedb.database.Binary;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ha.HAServer;

/**
 * Response for forwarded transaction.
 */
public class ErrorResponse extends HAAbstractCommand {
  public String exceptionClass;
  public String exceptionMessage;

  public ErrorResponse() {
  }

  public ErrorResponse(final Exception exception) {
    this.exceptionClass = exception.getClass().getName();
    this.exceptionMessage = exception.getMessage();
  }

  @Override
  public HACommand execute(final HAServer server, final String remoteServerName, final long messageNumber) {
    server.receivedResponseFromForward(messageNumber, null, this);
    return null;
  }

  @Override
  public void toStream(final Binary stream) {
    stream.putString(exceptionClass);
    stream.putString(exceptionMessage);
  }

  @Override
  public void fromStream(ArcadeDBServer server, final Binary stream) {
    exceptionClass = stream.getString();
    exceptionMessage = stream.getString();
  }

  @Override
  public String toString() {
    return "error-response(" + exceptionClass + ")";
  }
}
