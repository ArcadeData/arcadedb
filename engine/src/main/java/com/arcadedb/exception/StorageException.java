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
package com.arcadedb.exception;

/**
 * Exception thrown when storage and I/O errors occur.
 * <p>
 * This exception category covers:
 * <ul>
 *   <li>File system I/O errors (read/write failures)</li>
 *   <li>Data corruption detection</li>
 *   <li>Write-ahead log (WAL) errors</li>
 *   <li>Serialization/deserialization errors</li>
 *   <li>Encryption/decryption errors</li>
 *   <li>Backup and restore errors</li>
 * </ul>
 * <p>
 * Example usage:
 * <pre>{@code
 * try {
 *     fileChannel.write(buffer);
 * } catch (IOException e) {
 *     throw new StorageException(ErrorCode.IO_ERROR, "Failed to write to file")
 *         .withContext("filePath", file.getAbsolutePath())
 *         .withContext("bufferSize", buffer.remaining())
 *         .withContext("bytesWritten", bytesWritten);
 * }
 * }</pre>
 *
 * @since 25.12
 * @see ErrorCode
 * @see ArcadeDBException
 */
public class StorageException extends ArcadeDBException {

  /**
   * Constructs a new storage exception with the specified error code and message.
   *
   * @param errorCode the error code
   * @param message   the detail message
   */
  public StorageException(final ErrorCode errorCode, final String message) {
    super(errorCode, message);
  }

  /**
   * Constructs a new storage exception with the specified error code, message, and cause.
   *
   * @param errorCode the error code
   * @param message   the detail message
   * @param cause     the underlying cause (typically IOException)
   */
  public StorageException(final ErrorCode errorCode, final String message, final Throwable cause) {
    super(errorCode, message, cause);
  }

  /**
   * Returns the default error code for storage exceptions.
   *
   * @return IO_ERROR
   */
  @Override
  protected ErrorCode getDefaultErrorCode() {
    return ErrorCode.IO_ERROR;
  }

  /**
   * Returns the HTTP status code for this exception.
   * Storage exceptions typically map to 500 (Internal Server Error),
   * except for corruption which may indicate 503 (Service Unavailable).
   *
   * @return the HTTP status code
   */
  @Override
  public int getHttpStatus() {
    return switch (getErrorCode()) {
      case CORRUPTION_DETECTED -> 503; // Service Unavailable
      case IO_ERROR, WAL_ERROR, SERIALIZATION_ERROR, ENCRYPTION_ERROR -> 500; // Internal Server Error
      case BACKUP_ERROR, RESTORE_ERROR -> 500; // Internal Server Error
      default -> 500;
    };
  }
}
