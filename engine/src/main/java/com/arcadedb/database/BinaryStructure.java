/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.arcadedb.database;

import java.nio.ByteBuffer;

public interface BinaryStructure {
  void append(Binary toCopy);

  int position();

  void position(int index);

  void putByte(int index, byte value);

  void putByte(byte value);

  int putNumber(int index, long value);

  int putNumber(long value);

  int putUnsignedNumber(int index, long value);

  int putUnsignedNumber(long value);

  void putShort(int index, short value);

  void putShort(short value);

  void putInt(int index, int value);

  void putInt(int value);

  void putLong(int index, long value);

  void putLong(long value);

  int putString(int index, String value);

  int putString(String value);

  int putBytes(int index, byte[] value);

  int putBytes(byte[] value);

  int putBytes(byte[] value, int size);

  void putByteArray(int index, byte[] value);

  void putByteArray(int index, byte[] value, int length);

  void putByteArray(byte[] value);

  void putBuffer(ByteBuffer value);

  void putByteArray(byte[] value, int length);

  byte getByte(int index);

  byte getByte();

  long[] getNumberAndSize(int index);

  long getNumber();

  long getUnsignedNumber();

  long[] getUnsignedNumberAndSize();

  short getShort(int index);

  short getShort();

  short getUnsignedShort();

  int getInt();

  int getInt(int index);

  long getLong();

  long getLong(int index);

  String getString();

  String getString(int index);

  void getByteArray(byte[] buffer);

  void getByteArray(int index, byte[] buffer);

  void getByteArray(int index, byte[] buffer, int offset, int length);

  byte[] getBytes();

  byte[] getBytes(int index);

  byte[] toByteArray();

  byte[] remainingToByteArray();

  ByteBuffer getByteBuffer();

  int size();
}
