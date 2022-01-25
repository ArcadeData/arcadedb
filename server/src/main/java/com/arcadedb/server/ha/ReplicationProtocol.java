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

public class ReplicationProtocol extends Thread {
  public static final long  MAGIC_NUMBER     = 20986405762943483L;
  public static final short PROTOCOL_VERSION = 0;

  // MESSAGES
  public static final short COMMAND_CONNECT            = 0;
  public static final short COMMAND_VOTE_FOR_ME        = 1;
  public static final short COMMAND_ELECTION_COMPLETED = 2;

  // CONNECT ERROR
  public static final byte ERROR_CONNECT_NOLEADER            = 0;
  public static final byte ERROR_CONNECT_ELECTION_PENDING    = 1;
  public static final byte ERROR_CONNECT_UNSUPPORTEDPROTOCOL = 2;
  public static final byte ERROR_CONNECT_WRONGCLUSTERNAME    = 3;
  public static final byte ERROR_CONNECT_SAME_SERVERNAME     = 4;
}
