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
package com.arcadedb.server.http;

import com.arcadedb.exception.ArcadeDBException;
import com.arcadedb.exception.ErrorCode;
import com.arcadedb.network.exception.NetworkErrorCode;
import com.arcadedb.network.exception.NetworkException;
import com.arcadedb.serializer.json.JSONObject;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;

/**
 * Translates ArcadeDB exceptions to HTTP status codes and response bodies.
 * <p>
 * This is the ONLY place in the codebase where exceptions are mapped to HTTP concepts.
 * This maintains proper architectural layering by keeping HTTP knowledge exclusively in
 * the server module, preventing contamination of lower-level modules (engine, network)
 * with HTTP-specific logic.
 * <p>
 * <strong>Architecture Principle:</strong>
 * <pre>
 *   Engine Module (ArcadeDBException)
 *        ↓
 *   Network Module (NetworkException)
 *        ↓
 *   Server Module (HttpExceptionTranslator) ← Only place HTTP mapping happens
 * </pre>
 * <p>
 * <strong>HTTP Status Code Mapping Strategy:</strong>
 * <ul>
 *   <li>4xx - Client errors (bad request, not found, conflict, etc.)</li>
 *   <li>5xx - Server errors (internal server error, unavailable, etc.)</li>
 *   <li>Special status codes: 307 (Redirect to leader), 408 (Timeout), 503 (Service unavailable)</li>
 * </ul>
 * <p>
 * <strong>Usage Examples:</strong>
 * <pre>{@code
 * // Direct status code retrieval
 * int status = HttpExceptionTranslator.getHttpStatus(exception);
 *
 * // JSON error response generation
 * String jsonBody = HttpExceptionTranslator.toJsonResponse(exception);
 *
 * // Complete error response with status and body
 * HttpExceptionTranslator.sendError(httpExchange, exception);
 * }</pre>
 *
 * @since 26.1
 * @see ArcadeDBException
 * @see NetworkException
 * @see ErrorCode
 * @see NetworkErrorCode
 */
public class HttpExceptionTranslator {

  private HttpExceptionTranslator() {
    // Utility class - prevent instantiation
  }

  /**
   * Returns the appropriate HTTP status code for an exception.
   * <p>
   * This method replaces the deprecated {@code ArcadeDBException.getHttpStatus()} method
   * that was previously in the engine module. By moving this logic to the server module,
   * we maintain proper architectural separation.
   * <p>
   * The mapping strategy:
   * <ol>
   *   <li>Check if exception is a {@link NetworkException} → call getHttpStatusForNetworkError()</li>
   *   <li>Check if exception is an {@link ArcadeDBException} → call getHttpStatusForErrorCode()</li>
   *   <li>Default: return 500 (Internal Server Error)</li>
   * </ol>
   *
   * @param throwable the exception to translate (can be null)
   * @return the HTTP status code (e.g., 404, 500, 503)
   *
   * @example
   * <pre>{@code
   * try {
   *     // Database operation
   * } catch (Exception e) {
   *     int statusCode = HttpExceptionTranslator.getHttpStatus(e);
   *     response.setStatusCode(statusCode);
   * }
   * }</pre>
   */
  public static int getHttpStatus(final Throwable throwable) {
    // Handle network exceptions first (they are more specific)
    if (throwable instanceof NetworkException netEx) {
      return getHttpStatusForNetworkError(netEx.getNetworkErrorCode());
    }

    // Handle ArcadeDB engine exceptions
    if (throwable instanceof ArcadeDBException arcadeEx) {
      return getHttpStatusForErrorCode(arcadeEx.getErrorCode());
    }

    // Default: Internal Server Error
    return 500;
  }

  /**
   * Maps engine error codes to appropriate HTTP status codes.
   * <p>
   * This private method is the single source of truth for HTTP status code assignment
   * for all engine-level error codes. The mapping follows RESTful conventions and
   * HTTP specifications.
   * <p>
   * <strong>Mapping Categories:</strong>
   * <ul>
   *   <li><strong>404 Not Found:</strong> Database/Type/Property/Index not found</li>
   *   <li><strong>400 Bad Request:</strong> Invalid input, syntax errors, configuration errors</li>
   *   <li><strong>409 Conflict:</strong> Resource already exists, concurrent modification, duplicate key</li>
   *   <li><strong>403 Forbidden:</strong> Permission denied, database closed/readonly</li>
   *   <li><strong>408 Request Timeout:</strong> Transaction or lock timeout</li>
   *   <li><strong>422 Unprocessable Entity:</strong> Validation or serialization errors</li>
   *   <li><strong>501 Not Implemented:</strong> Feature not available</li>
   *   <li><strong>503 Service Unavailable:</strong> Retry needed (optimistic locking)</li>
   *   <li><strong>500 Internal Server Error:</strong> Unexpected server errors</li>
   * </ul>
   *
   * @param errorCode the engine error code to map
   * @return the HTTP status code
   *
   * @see ErrorCode
   */
  private static int getHttpStatusForErrorCode(final ErrorCode errorCode) {
    return switch (errorCode) {
      // ========== Database Errors (DB_*) ==========
      case DB_NOT_FOUND -> 404; // Not Found - database doesn't exist
      case DB_ALREADY_EXISTS -> 409; // Conflict - database already exists
      case DB_IS_READONLY, DB_IS_CLOSED -> 403; // Forbidden - cannot write to DB
      case DB_INVALID_INSTANCE, DB_CONFIG_ERROR -> 400; // Bad Request - invalid configuration
      case DB_METADATA_ERROR, DB_OPERATION_ERROR -> 500; // Internal error during DB operation

      // ========== Transaction Errors (TX_*) ==========
      case TX_TIMEOUT, TX_LOCK_TIMEOUT -> 408; // Request Timeout - transaction took too long
      case TX_CONFLICT, TX_CONCURRENT_MODIFICATION -> 409; // Conflict - concurrent modification detected
      case TX_RETRY_NEEDED -> 503; // Service Unavailable - client should retry
      case TX_ERROR -> 500; // Internal transaction error

      // ========== Query Errors (QUERY_*) ==========
      case QUERY_SYNTAX_ERROR, QUERY_PARSING_ERROR -> 400; // Bad Request - syntax error in query
      case QUERY_EXECUTION_ERROR, QUERY_COMMAND_ERROR, QUERY_FUNCTION_ERROR -> 500; // Internal error during query execution

      // ========== Security Errors (SEC_*) ==========
      case SEC_UNAUTHORIZED, SEC_AUTHENTICATION_FAILED -> 401; // Unauthorized - authentication required
      case SEC_FORBIDDEN, SEC_AUTHORIZATION_FAILED -> 403; // Forbidden - permission denied

      // ========== Storage Errors (STORAGE_*) ==========
      case STORAGE_IO_ERROR, STORAGE_CORRUPTION, STORAGE_WAL_ERROR -> 500; // Internal error - storage failure
      case STORAGE_SERIALIZATION_ERROR -> 422; // Unprocessable Entity - data format error
      case STORAGE_ENCRYPTION_ERROR, STORAGE_BACKUP_ERROR, STORAGE_RESTORE_ERROR -> 500; // Internal error

      // ========== Schema Errors (SCHEMA_*) ==========
      case SCHEMA_TYPE_NOT_FOUND, SCHEMA_PROPERTY_NOT_FOUND -> 404; // Not Found - type/property doesn't exist
      case SCHEMA_VALIDATION_ERROR -> 422; // Unprocessable Entity - data validation failed
      case SCHEMA_ERROR -> 400; // Bad Request - invalid schema definition

      // ========== Index Errors (INDEX_*) ==========
      case INDEX_NOT_FOUND -> 404; // Not Found - index doesn't exist
      case INDEX_DUPLICATE_KEY -> 409; // Conflict - unique constraint violation
      case INDEX_ERROR -> 500; // Internal error during index operation

      // ========== Graph Errors (GRAPH_*) ==========
      case GRAPH_ALGORITHM_ERROR -> 500; // Internal error during graph computation

      // ========== Import/Export Errors ==========
      case IMPORT_ERROR, EXPORT_ERROR -> 500; // Internal error during import/export

      // ========== Internal Errors ==========
      case INTERNAL_ERROR -> 500; // Internal Server Error - unexpected condition

      // Default fallback (should not reach here)
      default -> 500;
    };
  }

  /**
   * Maps network error codes to appropriate HTTP status codes.
   * <p>
   * This private method maps network-layer error codes to HTTP status codes.
   * Network errors typically indicate connectivity issues, replication problems,
   * or protocol incompatibilities.
   * <p>
   * <strong>Mapping Categories:</strong>
   * <ul>
   *   <li><strong>400 Bad Request:</strong> Protocol violations or invalid messages</li>
   *   <li><strong>307 Temporary Redirect:</strong> Not leader - client should redirect to leader</li>
   *   <li><strong>502 Bad Gateway:</strong> Remote server error</li>
   *   <li><strong>503 Service Unavailable:</strong> Connection problems, quorum not reached</li>
   *   <li><strong>504 Gateway Timeout:</strong> Connection timeout</li>
   *   <li><strong>500 Internal Server Error:</strong> Replication or channel errors</li>
   * </ul>
   *
   * @param networkErrorCode the network-specific error code to map
   * @return the HTTP status code
   *
   * @see NetworkErrorCode
   */
  private static int getHttpStatusForNetworkError(final NetworkErrorCode networkErrorCode) {
    return switch (networkErrorCode) {
      // ========== Connection Errors ==========
      case CONNECTION_ERROR, CONNECTION_LOST, CONNECTION_CLOSED -> 503; // Service Unavailable - cannot connect
      case CONNECTION_TIMEOUT -> 504; // Gateway Timeout - connection attempt timed out

      // ========== Protocol Errors ==========
      case PROTOCOL_ERROR, PROTOCOL_INVALID_MESSAGE, PROTOCOL_VERSION_MISMATCH -> 400; // Bad Request - protocol violation

      // ========== Replication Errors ==========
      case REPLICATION_ERROR, REPLICATION_SYNC_ERROR -> 500; // Internal error during replication
      case REPLICATION_QUORUM_NOT_REACHED -> 503; // Service Unavailable - not enough nodes
      case REPLICATION_NOT_LEADER -> 307; // Temporary Redirect - client should connect to leader

      // ========== Remote Errors ==========
      case REMOTE_ERROR, REMOTE_SERVER_ERROR -> 502; // Bad Gateway - remote server error

      // ========== Channel Errors ==========
      case CHANNEL_CLOSED, CHANNEL_ERROR -> 503; // Service Unavailable - channel unavailable

      // Default fallback (should not reach here)
      default -> 500;
    };
  }

  /**
   * Creates a JSON error response from an exception.
   * <p>
   * This method generates a standardized JSON representation of an exception,
   * suitable for sending as an HTTP response body. The JSON includes error code,
   * category, message, timestamp, context, and cause information.
   * <p>
   * <strong>JSON Response Format:</strong>
   * <pre>{@code
   * {
   *   "error": "ERROR_CODE_NAME",
   *   "category": "Category Name",
   *   "message": "Human-readable error message",
   *   "timestamp": 1704834567890,
   *   "context": {
   *     "additionalInfo": "For debugging"
   *   },
   *   "cause": "ExceptionClass: Cause message"
   * }
   * }</pre>
   * <p>
   * <strong>Error Type Handling:</strong>
   * <ul>
   *   <li>{@link NetworkException} - Uses network error code with "Network" category</li>
   *   <li>{@link ArcadeDBException} - Uses engine error code with appropriate category</li>
   *   <li>Unknown exceptions - Uses "INTERNAL_ERROR" with class name</li>
   * </ul>
   *
   * @param throwable the exception to serialize
   * @return JSON string representation of the error
   *
   * @example
   * <pre>{@code
   * String jsonBody = HttpExceptionTranslator.toJsonResponse(
   *     new DatabaseException(ErrorCode.DB_NOT_FOUND, "Database not found")
   *         .withContext("database", "mydb")
   * );
   * // Returns: {"error":"DB_NOT_FOUND","category":"Database",
   * //           "message":"Database not found","timestamp":...,
   * //           "context":{"database":"mydb"}}
   * }</pre>
   *
   * @see ArcadeDBException#toJSON()
   * @see NetworkException#toJSON()
   */
  public static String toJsonResponse(final Throwable throwable) {
    final JSONObject json = new JSONObject();

    if (throwable instanceof NetworkException netEx) {
      // Network exception - use network-specific error code
      json.put("error", netEx.getNetworkErrorCodeName());
      json.put("category", "Network");
      json.put("message", netEx.getMessage());
      json.put("timestamp", netEx.getTimestamp());

      if (!netEx.getContext().isEmpty()) {
        json.put("context", new JSONObject(netEx.getContext()));
      }

    } else if (throwable instanceof ArcadeDBException arcadeEx) {
      // Engine exception - use engine error code with category
      json.put("error", arcadeEx.getErrorCodeName());
      json.put("category", arcadeEx.getErrorCategoryName());
      json.put("message", arcadeEx.getMessage());
      json.put("timestamp", arcadeEx.getTimestamp());

      if (!arcadeEx.getContext().isEmpty()) {
        json.put("context", new JSONObject(arcadeEx.getContext()));
      }

    } else {
      // Unknown exception - provide basic information
      json.put("error", "INTERNAL_ERROR");
      json.put("category", "Internal");
      json.put("message", throwable != null ? throwable.getMessage() : "Unknown error");
      json.put("type", throwable != null ? throwable.getClass().getSimpleName() : "Unknown");
    }

    // Add cause information if present
    if (throwable != null && throwable.getCause() != null) {
      final Throwable cause = throwable.getCause();
      json.put("cause", cause.getClass().getSimpleName() + ": " + cause.getMessage());
    }

    return json.toString();
  }

  /**
   * Sends a complete error response to an HTTP client.
   * <p>
   * This is a convenience method that combines status code mapping and JSON response generation,
   * simplifying error handling in HTTP handlers.
   * <p>
   * This method:
   * <ol>
   *   <li>Determines the appropriate HTTP status code via {@link #getHttpStatus(Throwable)}</li>
   *   <li>Generates a JSON error response via {@link #toJsonResponse(Throwable)}</li>
   *   <li>Sets the response status code</li>
   *   <li>Sets content-type to {@code application/json}</li>
   *   <li>Sets character encoding to {@code UTF-8}</li>
   *   <li>Writes the JSON body to the response</li>
   * </ol>
   * <p>
   * <strong>HTTP Handler Usage Pattern (Before):</strong>
   * <pre>{@code
   * try {
   *     // Handler logic
   * } catch (ArcadeDBException e) {
   *     response.setStatus(e.getHttpStatus());  // ❌ Engine knows about HTTP
   *     response.setContentType("application/json");
   *     response.write(e.toJSON());
   * }
   * }</pre>
   * <p>
   * <strong>HTTP Handler Usage Pattern (After):</strong>
   * <pre>{@code
   * try {
   *     // Handler logic
   * } catch (Exception e) {
   *     HttpExceptionTranslator.sendError(exchange, e);  // ✅ Clean translation
   * }
   * }</pre>
   *
   * @param exchange the HTTP server exchange object for sending the response
   * @param throwable the exception to send as error response
   *
   * @throws NullPointerException if exchange is null
   *
   * @example
   * <pre>{@code
   * public void handleRequest(HttpServerExchange exchange) {
   *     try {
   *         Database db = server.getDatabase("mydb");
   *         // Process request...
   *     } catch (Exception e) {
   *         HttpExceptionTranslator.sendError(exchange, e);
   *     }
   * }
   * }</pre>
   *
   * @see #getHttpStatus(Throwable)
   * @see #toJsonResponse(Throwable)
   */
  public static void sendError(final HttpServerExchange exchange, final Throwable throwable) {
    final int statusCode = getHttpStatus(throwable);
    final String jsonBody = toJsonResponse(throwable);

    exchange.setStatusCode(statusCode);
    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json;charset=UTF-8");

    exchange.getResponseSender().send(jsonBody);
  }
}
