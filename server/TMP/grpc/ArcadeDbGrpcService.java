package com.arcadedb.server.grpc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.ArcadeDBServer;
import com.google.protobuf.Empty;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Value;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

/**
 * gRPC Service implementation for ArcadeDB
 */
public class ArcadeDbGrpcService extends ArcadeDbServiceGrpc.ArcadeDbServiceImplBase {
    
    private static final Logger logger = LoggerFactory.getLogger(ArcadeDbGrpcService.class);
    
    // Transaction management
    private final Map<String, Database> activeTransactions = new ConcurrentHashMap<>();
    
    // Database connection pool
    private final Map<String, Database> databasePool = new ConcurrentHashMap<>();
    
	// Track highest acknowledged chunk per session for idempotency
	private final Map<String, Long> sessionWatermark = new ConcurrentHashMap<>();

    
    // ArcadeDB server reference (optional, for accessing existing databases)
    private final ArcadeDBServer arcadeServer;
    
    // Database directory path
    private final String databasePath;
    
    public ArcadeDbGrpcService(String databasePath, ArcadeDBServer server) {
        this.databasePath = databasePath;
        this.arcadeServer = server;
    }
    
    public void close() {
        // Close all open databases
        for (Database db : databasePool.values()) {
            try {
                if (db != null && db.isOpen()) {
                    db.close();
                }
            } catch (Exception e) {
                logger.error("Error closing database", e);
            }
        }
        databasePool.clear();
        
        // Clean up transactions
        for (Database db : activeTransactions.values()) {
            try {
                if (db != null && db.isOpen()) {
                    db.rollback();
                    db.close();
                }
            } catch (Exception e) {
                logger.error("Error closing transaction database", e);
            }
        }
        activeTransactions.clear();
    }
    
    @Override
    public void createDatabase(CreateDatabaseRequest request, 
                               StreamObserver<CreateDatabaseResponse> responseObserver) {
        try {
            // Validate credentials if needed
            validateCredentials(request.getCredentials());
            
            // Create new database factory for this specific database
            DatabaseFactory dbFactory = new DatabaseFactory(databasePath + "/" + request.getDatabaseName());
            
            // Create the database
            Database database = dbFactory.create();
            database.close();
            
            CreateDatabaseResponse response = CreateDatabaseResponse.newBuilder()
                .setSuccess(true)
                .setMessage("Database created successfully")
                .setDatabaseId(request.getDatabaseName())
                .build();
                
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            
        } catch (Exception e) {
            logger.error("Error creating database: {}", e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL
                .withDescription("Failed to create database: " + e.getMessage())
                .asException());
        }
    }
    
    @Override
    public void executeQuery(ExecuteQueryRequest request, 
                             StreamObserver<ExecuteQueryResponse> responseObserver) {
        try {
        	
            // Force compression for streaming (usually beneficial)
            CompressionAwareService.setResponseCompression(responseObserver, "gzip");
            
            Database database = getDatabase(request.getDatabase(), request.getCredentials());
            
            // Check if this is part of a transaction
            if (request.hasTransaction()) {
                database = activeTransactions.get(request.getTransaction().getTransactionId());
                if (database == null) {
                    throw new IllegalArgumentException("Invalid transaction ID");
                }
            }
            
            // Execute the query
            long startTime = System.currentTimeMillis();
            ResultSet resultSet = database.query("sql", request.getQuery(), 
                                                 convertParameters(request.getParametersMap()));
            
            logger.debug("executeQuery(): to get resultSet = {}", (System.currentTimeMillis() - startTime));
            
            // Build response
            QueryResult.Builder resultBuilder = QueryResult.newBuilder();
            
            // Process results
            int count = 0;
            while (resultSet.hasNext()) {
                Result result = resultSet.next();
                if (result.isElement()) {
                    com.arcadedb.database.Record dbRecord = result.getElement().get();
                    Record grpcRecord = convertToGrpcRecord(dbRecord);
                    resultBuilder.addRecords(grpcRecord);
                    count++;
                    
                    // Apply limit if specified
                    if (request.getLimit() > 0 && count >= request.getLimit()) {
                        break;
                    }
                }
            }
            
            resultBuilder.setTotalRecordsInBatch(count);
            
            long executionTime = System.currentTimeMillis() - startTime;

            ExecuteQueryResponse response = ExecuteQueryResponse.newBuilder()
                .addResults(resultBuilder.build())
                .setExecutionTimeMs(executionTime)
                .build();
                
            logger.debug("executeQuery(): executionTime + response generation = {}", executionTime);
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            
        } catch (Exception e) {
            logger.error("Error executing query: {}", e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL
                .withDescription("Query execution failed: " + e.getMessage())
                .asException());
        }
    }
    
    @Override
    public void beginTransaction(BeginTransactionRequest request, 
                                 StreamObserver<BeginTransactionResponse> responseObserver) {
        try {
            Database database = getDatabase(request.getDatabase(), request.getCredentials());
            
            // Begin transaction
            database.begin();
            
            // Generate transaction ID
            String transactionId = generateTransactionId();
            activeTransactions.put(transactionId, database);
            
            BeginTransactionResponse response = BeginTransactionResponse.newBuilder()
                .setTransactionId(transactionId)
                .setTimestamp(System.currentTimeMillis())
                .build();
                
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            
        } catch (Exception e) {
            logger.error("Error beginning transaction: {}", e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL
                .withDescription("Failed to begin transaction: " + e.getMessage())
                .asException());
        }
    }
    
    @Override
    public void commitTransaction(CommitTransactionRequest request, 
                                  StreamObserver<CommitTransactionResponse> responseObserver) {
        try {
            Database database = activeTransactions.remove(request.getTransaction().getTransactionId());
            if (database == null) {
                throw new IllegalArgumentException("Invalid transaction ID");
            }
            
            // Commit transaction
            database.commit();
            
            CommitTransactionResponse response = CommitTransactionResponse.newBuilder()
                .setSuccess(true)
                .setMessage("Transaction committed successfully")
                .setTimestamp(System.currentTimeMillis())
                .build();
                
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            
        } catch (Exception e) {
            logger.error("Error committing transaction: {}", e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL
                .withDescription("Failed to commit transaction: " + e.getMessage())
                .asException());
        }
    }
    
    @Override
    public void rollbackTransaction(RollbackTransactionRequest request, 
                                    StreamObserver<RollbackTransactionResponse> responseObserver) {
        try {
            Database database = activeTransactions.remove(request.getTransaction().getTransactionId());
            if (database == null) {
                throw new IllegalArgumentException("Invalid transaction ID");
            }
            
            // Rollback transaction
            database.rollback();
            
            RollbackTransactionResponse response = RollbackTransactionResponse.newBuilder()
                .setSuccess(true)
                .setMessage("Transaction rolled back successfully")
                .build();
                
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            
        } catch (Exception e) {
            logger.error("Error rolling back transaction: {}", e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL
                .withDescription("Failed to rollback transaction: " + e.getMessage())
                .asException());
        }
    }
    
    @Override
    public void streamQuery(StreamQueryRequest request, StreamObserver<QueryResult> responseObserver) {
      final ServerCallStreamObserver<QueryResult> scso =
          (ServerCallStreamObserver<QueryResult>) responseObserver;

      final AtomicBoolean cancelled = new AtomicBoolean(false);
      scso.setOnCancelHandler(() -> cancelled.set(true));

      try {
        Database database = getDatabase(request.getDatabase(), request.getCredentials());
        final int batchSize = Math.max(1, request.getBatchSize());

        // Dispatch on mode
        switch (request.getRetrievalMode()) {
          case MATERIALIZE_ALL -> streamMaterialized(database, request, batchSize, scso, cancelled);
          case PAGED          -> streamPaged(database, request, batchSize, scso, cancelled);
          case CURSOR -> streamCursor(database, request, batchSize, scso, cancelled);
          default -> streamCursor(database, request, batchSize, scso, cancelled);            
        }

        if (!cancelled.get()) scso.onCompleted();
      } catch (Exception e) {
        if (!cancelled.get()) {
          responseObserver.onError(
              Status.INTERNAL.withDescription("Stream query failed: " + e.getMessage()).asException());
        }
      }
    }
    
    /** Mode 1 (existing behavior-ish): run once and iterate results, batching as we go. */
	private void streamCursor(Database db, StreamQueryRequest req, int batchSize, ServerCallStreamObserver<QueryResult> scso,
			AtomicBoolean cancelled) {
		long running = 0L;
		QueryResult.Builder batch = QueryResult.newBuilder();
		int inBatch = 0;

		try (ResultSet rs = db.query("sql", req.getQuery(), convertParameters(req.getParametersMap()))) {
			while (rs.hasNext()) {
				if (cancelled.get())
					return;
				waitUntilReady(scso, cancelled);

				Result r = rs.next();
				if (!r.isElement())
					continue;

				com.arcadedb.database.Record rec = r.getElement().get();
				batch.addRecords(convertToGrpcRecord(rec));
				inBatch++;
				running++;

				if (inBatch >= batchSize) {
					safeOnNext(scso, cancelled,
							batch.setTotalRecordsInBatch(inBatch).setRunningTotalEmitted(running).setIsLastBatch(false).build());
					batch = QueryResult.newBuilder();
					inBatch = 0;
				}
			}

			if (!cancelled.get() && inBatch > 0) {
				safeOnNext(scso, cancelled, batch.setTotalRecordsInBatch(inBatch).setRunningTotalEmitted(running).setIsLastBatch(true).build());
			}
		}
	}

    /** Mode 2: materialize everything first (simple, but can be memory-heavy). */
	private void streamMaterialized(Database db, StreamQueryRequest req, int batchSize, ServerCallStreamObserver<QueryResult> scso,
			AtomicBoolean cancelled) {
		final java.util.ArrayList<Record> all = new java.util.ArrayList<>();
		try (ResultSet rs = db.query("sql", req.getQuery(), convertParameters(req.getParametersMap()))) {
			while (rs.hasNext()) {
				if (cancelled.get())
					return;
				Result r = rs.next();
				if (!r.isElement())
					continue;
				all.add(convertToGrpcRecord(r.getElement().get()));
			}
		}

		long running = 0L;
		for (int i = 0; i < all.size(); i += batchSize) {
			if (cancelled.get())
				return;
			waitUntilReady(scso, cancelled);

			int end = Math.min(i + batchSize, all.size());
			QueryResult.Builder b = QueryResult.newBuilder();
			for (int j = i; j < end; j++)
				b.addRecords(all.get(j));

			running += (end - i);
			safeOnNext(scso, cancelled,
					b.setTotalRecordsInBatch(end - i).setRunningTotalEmitted(running).setIsLastBatch(end == all.size()).build());
		}
	}

    /** Mode 3: only fetch one page’s worth of rows per emission via LIMIT/SKIP. */
	private void streamPaged(Database db, StreamQueryRequest req, int batchSize, ServerCallStreamObserver<QueryResult> scso,
			AtomicBoolean cancelled) {

		final String pagedSql = wrapWithSkipLimit(req.getQuery()); // see helper below
		int offset = 0;
		long running = 0L;

		while (true) {
			if (cancelled.get())
				return;
			waitUntilReady(scso, cancelled);

			java.util.Map<String, Object> params = new java.util.HashMap<>(convertParameters(req.getParametersMap()));
			params.put("_skip", offset);
			params.put("_limit", batchSize);

			int count = 0;
			QueryResult.Builder b = QueryResult.newBuilder();

			try (ResultSet rs = db.query("sql", pagedSql, params)) {
				while (rs.hasNext()) {
					if (cancelled.get())
						return;
					Result r = rs.next();
					if (!r.isElement())
						continue;
					b.addRecords(convertToGrpcRecord(r.getElement().get()));
					count++;
				}
			}

			if (count == 0)
				return; // no more rows

			running += count;
			boolean last = count < batchSize;

			safeOnNext(scso, cancelled, b.setTotalRecordsInBatch(count).setRunningTotalEmitted(running).setIsLastBatch(last).build());

			if (last)
				return;
			offset += batchSize;
		}
	}

    /** Wrap arbitrary SQL so we can safely inject LIMIT/SKIP outside. */
    private String wrapWithSkipLimit(String originalSql) {
        
    	// Minimal defensive approach; you can do a real parser if needed.
    	
    	// ArcadeDB: SELECT FROM (...) [ORDER BY ...] SKIP :_skip LIMIT :_limit

    	return "SELECT FROM (" + originalSql + ") ORDER BY @rid SKIP :_skip LIMIT :_limit";
    }
    
	private void safeOnNext(ServerCallStreamObserver<QueryResult> scso, AtomicBoolean cancelled, QueryResult payload) {
		if (cancelled.get())
			return;
		try {
			scso.onNext(payload);
		}
		catch (StatusRuntimeException e) {
			if (e.getStatus().getCode() == Status.Code.CANCELLED) {
				cancelled.set(true);
				return;
			}
			throw e;
		}
	}

	private void waitUntilReady(ServerCallStreamObserver<?> scso, AtomicBoolean cancelled) {
		// Skip if you’re okay with best-effort pushes; otherwise honor transport readiness
		if (scso.isReady())
			return;
		while (!scso.isReady()) {
			if (cancelled.get())
				return;
			// avoid burning CPU:
			try {
				Thread.sleep(1);
			}
			catch (InterruptedException ie) {
				Thread.currentThread().interrupt();
				return;
			}
		}
	}

	// --- 1) Unary bulk ---
	@Override
	public void bulkInsert(BulkInsertRequest req, StreamObserver<InsertSummary> resp) {
		final InsertOptions opts = defaults(req.getOptions());
		try (InsertContext ctx = new InsertContext(opts)) {
			insertRows(ctx, req.getRowsList().iterator());
			ctx.flushCommit(true);
			resp.onNext(ctx.summary());
			resp.onCompleted();
		}
		catch (Exception e) {
			resp.onError(Status.INTERNAL.withDescription("bulkInsert: " + e.getMessage()).asException());
		}
	}

	// --- 2) Client-streaming; single summary at end ---
	@Override
	public StreamObserver<InsertChunk> insertStream(StreamObserver<InsertSummary> resp) {
		final ServerCallStreamObserver<InsertSummary> call = (ServerCallStreamObserver<InsertSummary>) resp;
		call.disableAutoInboundFlowControl();

		// You can pass options via the first chunk's session_id, or set server
		// defaults; here: server defaults
		final InsertOptions opts = defaults(InsertOptions.newBuilder().build());
		final InsertContext ctx;
		try {
			ctx = new InsertContext(opts);
		}
		catch (Exception e) {
			resp.onError(Status.FAILED_PRECONDITION.withDescription(e.getMessage()).asException());
			return new StreamObserver<>() {
				public void onNext(InsertChunk c) {
				}

				public void onError(Throwable t) {
				}

				public void onCompleted() {
				}
			};
		}

		return new StreamObserver<>() {
			@Override
			public void onNext(InsertChunk c) {
				insertRows(ctx, c.getRowsList().iterator());
				call.request(1);
			}

			@Override
			public void onError(Throwable t) {
				ctx.closeQuietly();
			}

			@Override
			public void onCompleted() {
				try {
					ctx.flushCommit(true);
					resp.onNext(ctx.summary());
					resp.onCompleted();
				}
				catch (Exception e) {
					resp.onError(Status.INTERNAL.withDescription("insertStream: " + e.getMessage()).asException());
				}
				finally {
					ctx.closeQuietly();
				}
			}
		};
	}

	// --- 3) Bi-di with per-batch ACKs ---
	@Override
	public StreamObserver<InsertRequest> insertBidirectional(StreamObserver<InsertResponse> resp) {
		final ServerCallStreamObserver<InsertResponse> call = (ServerCallStreamObserver<InsertResponse>) resp;
		call.disableAutoInboundFlowControl();

		final AtomicReference<InsertContext> ref = new AtomicReference<>();
		return new StreamObserver<>() {
			@Override
			public void onNext(InsertRequest req) {
				switch (req.getMsgCase()) {
				case START -> {
					InsertOptions opts = defaults(req.getStart().getOptions());
					InsertContext ctx = new InsertContext(opts);
					ref.set(ctx);
					sessionWatermark.put(ctx.sessionId, 0L);
					resp.onNext(InsertResponse.newBuilder().setStarted(Started.newBuilder().setSessionId(ctx.sessionId).build()).build());
					call.request(1);
				}
				case CHUNK -> {
					InsertContext ctx = require(ref.get(), "session not started");
					InsertChunk c = req.getChunk();

					// Idempotent replay guard
					long hi = sessionWatermark.getOrDefault(ctx.sessionId, 0L);
					if (c.getChunkSeq() <= hi) {
						resp.onNext(InsertResponse.newBuilder()
								.setBatchAck(BatchAck.newBuilder().setSessionId(ctx.sessionId).setChunkSeq(c.getChunkSeq()).build()).build());
						call.request(1);
						return;
					}

					BatchCounts counts = insertRows(ctx, c.getRowsList().iterator());
					sessionWatermark.put(ctx.sessionId, c.getChunkSeq());

					resp.onNext(InsertResponse.newBuilder()
							.setBatchAck(BatchAck.newBuilder().setSessionId(ctx.sessionId).setChunkSeq(c.getChunkSeq())
									.setInserted(counts.inserted).setUpdated(counts.updated).setIgnored(counts.ignored).setFailed(counts.failed)
									.addAllErrors(counts.errors))
							.build());
					call.request(1);
				}
				case COMMIT -> {
					InsertContext ctx = require(ref.get(), "session not started");
					try {
						ctx.flushCommit(true);
						resp.onNext(InsertResponse.newBuilder().setCommitted(Committed.newBuilder().setSummary(ctx.summary()).build()).build());
						resp.onCompleted();
					}
					catch (Exception e) {
						resp.onError(Status.INTERNAL.withDescription("commit: " + e.getMessage()).asException());
					}
					finally {
						sessionWatermark.remove(ctx.sessionId);
						ctx.closeQuietly();
					}
				}
				case MSG_NOT_SET -> {
					/* ignore */ }
				}
			}

			@Override
			public void onError(Throwable t) {
				InsertContext ctx = ref.get();
				if (ctx != null)
					ctx.closeQuietly();
			}

			@Override
			public void onCompleted() {
				/* if client half-closes without COMMIT, choose policy */ }
		};
	}

	// ---------- Core insert plumbing ----------

	private static final class BatchCounts {
		long inserted, updated, ignored, failed;
		final List<RowError> errors = new ArrayList<>();

		void addError(long rowIndex, String code, String msg, String field) {
			failed++;
			errors.add(RowError.newBuilder().setRowIndex(rowIndex).setCode(code).setMessage(msg).setField(field).build());
		}
	}

	private BatchCounts insertRows(InsertContext ctx, Iterator<Record> it) {
		
		BatchCounts c = new BatchCounts();
		
		int inBatch = 0;

		while (it.hasNext()) {
			Record r = it.next();
			ctx.received++;
			try {
				
				MutableDocument doc = ctx.db.newDocument(ctx.opts.getTargetClass());
				
				// map fields:
				applyGrpcRecordToDocument(r, doc);

				switch (ctx.opts.getConflictMode()) {
				case CONFLICT_ABORT -> {
					doc.save();
					c.inserted++;
				}
				case CONFLICT_IGNORE -> {
					try {
						doc.save();
						c.inserted++;
					}
					catch (Exception dup) {
						c.ignored++;
					}
				}
				case CONFLICT_UPDATE -> {
					if (tryUpsert(ctx, doc))
						c.updated++;
					else {
						doc.save();
						c.inserted++;
					}
				}
				}

			}
			catch (ValidationException ve) {
				c.addError(ctx.received - 1, "VALIDATION", ve.getMessage(), "");
			}
			catch (DuplicatedKeyException dke) {
				if (ctx.opts.getConflictMode() == ConflictMode.CONFLICT_ABORT) {
					c.addError(ctx.received - 1, "CONFLICT", dke.getMessage(), "");
				}
				else {
					c.ignored++;
				}
			}
			catch (Exception e) {
				c.addError(ctx.received - 1, "DB_ERROR", e.getMessage(), "");
			}

			inBatch++;
			if (ctx.opts.getTransactionMode() == TransactionMode.PER_BATCH && inBatch >= ctx.serverBatchSize()) {
				ctx.flushCommit(false);
				inBatch = 0;
			}
			else if (ctx.opts.getTransactionMode() == TransactionMode.PER_ROW) {
				ctx.flushCommit(false);
				inBatch = 0;
			}
		}
		
		return c;
	}

	private void applyGrpcRecordToDocument(Record r, MutableDocument doc) {
		// Example if proto = message Record { map<string, google.protobuf.Value>
		// properties = 1; }
		if (hasMethod(r, "getPropertiesMap")) {
			@SuppressWarnings("unchecked")
			Map<String, com.google.protobuf.Value> props = (Map<String, com.google.protobuf.Value>) invokeNoArg(r, "getPropertiesMap");
			props.forEach((k, v) -> doc.set(k, toJava(v)));
			return;
		}

		// Example if proto = message Record { repeated Property properties = 1; message
		// Property { string key=1; google.protobuf.Value value=2; } }
		if (hasMethod(r, "getPropertiesList")) {
			@SuppressWarnings("unchecked")
			List<Object> list = (List<Object>) invokeNoArg(r, "getPropertiesList");
			for (Object p : list) {
				String key = (String) invokeNoArg(p, "getKey");
				com.google.protobuf.Value val = (com.google.protobuf.Value) invokeNoArg(p, "getValue");
				doc.set(key, toJava(val));
			}
			return;
		}

		// Fallback: if your proto DOES have fields map after all
		if (hasMethod(r, "getFieldsMap")) {
			@SuppressWarnings("unchecked")
			Map<String, com.google.protobuf.Value> fields = (Map<String, com.google.protobuf.Value>) invokeNoArg(r, "getFieldsMap");
			fields.forEach((k, v) -> doc.set(k, toJava(v)));
		}
	}

	// tiny reflection helpers (compile-time safe across proto variants)
	private static boolean hasMethod(Object o, String name) {
		try {
			o.getClass().getMethod(name);
			return true;
		}
		catch (NoSuchMethodException e) {
			return false;
		}
	}

	private static Object invokeNoArg(Object o, String name) {
		try {
			return o.getClass().getMethod(name).invoke(o);
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
		
	private boolean tryUpsert(InsertContext ctx, MutableDocument doc) {
		
		if (ctx.keyCols.isEmpty())
			return false;
		
		String where = String.join(" AND ", ctx.keyCols.stream().map(k -> k + " = ?").toList());
		
		Object[] params = ctx.keyCols.stream().map(doc::get).toArray();

		try (ResultSet rs = ctx.db.query("sql", "SELECT FROM " + ctx.opts.getTargetClass() + " WHERE " + where, params)) {
		
			if (!rs.hasNext())
				return false;

			var res = rs.next();
			
			if (!res.isElement())
				return false;

			var existing = res.getElement().get();
			
			MutableDocument m = (MutableDocument) existing.asDocument(true);
			
			for (String col : ctx.updateCols) {
				m.set(col, doc.get(col));
			}
			
			m.save();
			
			return true;
		}
	}

	private static Object toJava(Value v) {
		return switch (v.getKindCase()) {
		case STRING_VALUE -> v.getStringValue();
		case NUMBER_VALUE -> v.getNumberValue();
		case BOOL_VALUE -> v.getBoolValue();
		case NULL_VALUE -> null;
		case STRUCT_VALUE -> new HashMap<>(v.getStructValue().getFieldsMap()); // or nested doc
		case LIST_VALUE -> v.getListValue().getValuesList().stream().map(ArcadeDbGrpcService::toJava).toList();
		case KIND_NOT_SET -> null;
		};
	}

	private InsertOptions defaults(InsertOptions in) {
		InsertOptions.Builder b = in.toBuilder();
		if (in.getServerBatchSize() == 0)
			b.setServerBatchSize(1000);
		if (in.getTransactionMode() == TransactionMode.UNRECOGNIZED)
			b.setTransactionMode(TransactionMode.PER_BATCH);
		return b.build();
	}

	private static <T> T require(T v, String msg) {
		if (v == null)
			throw Status.FAILED_PRECONDITION.withDescription(msg).asRuntimeException();
		return v;
	}

	private static Timestamp ts(long ms) {
		return Timestamp.newBuilder().setSeconds(ms / 1000).setNanos((int) ((ms % 1000) * 1_000_000)).build();
	}

	private final class InsertContext implements AutoCloseable {
		
		final InsertOptions opts;
		final Database db;
		final String sessionId = UUID.randomUUID().toString();
		final List<String> keyCols;
		final List<String> updateCols;
		long received = 0;
		final long started = System.currentTimeMillis();

		InsertContext(InsertOptions opts) {
			this.opts = opts;
			this.db = getDatabase(opts.getDatabase(), opts.getCredentials()); // your existing helper
			this.keyCols = opts.getKeyColumnsList();
			this.updateCols = opts.getUpdateColumnsOnConflictList();
			if (opts.getTransactionMode() == TransactionMode.PER_STREAM)
				db.begin();
		}

		int serverBatchSize() {
			return opts.getServerBatchSize();
		}

		void flushCommit(boolean end) {
			if (opts.getValidateOnly()) {
				if (end && opts.getTransactionMode() == TransactionMode.PER_STREAM)
					db.rollback();
				return;
			}
			if (opts.getTransactionMode() == TransactionMode.PER_BATCH) {
				db.commit();
				db.begin();
			}
			else if (end && opts.getTransactionMode() == TransactionMode.PER_STREAM)
				db.commit();
		}

		InsertSummary summary() {
			return InsertSummary.newBuilder().setReceived(received) // set by caller
					// inserted/updated/ignored/failed are tracked in BatchCounts -> you can total
					// them here if you prefer
					.setStartedAt(ts(started)).setFinishedAt(ts(System.currentTimeMillis())).build();
		}

		@Override
		public void close() {
		}

		void closeQuietly() {
			try {
				close();
			}
			catch (Exception ignore) {
			}
		}
	}
    
    @Override
    public void ping(Empty request, StreamObserver<PingResponse> responseObserver) {
        PingResponse response = PingResponse.newBuilder()
            .setTimestamp(System.currentTimeMillis())
            .setMessage("pong")
            .build();
            
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
    
    @Override
    public void getServerStatus(Empty request, StreamObserver<ServerStatusResponse> responseObserver) {
        try {
            ServerStatusResponse response = ServerStatusResponse.newBuilder()
                .setVersion("25.8.1-SNAPSHOT")
                .setStatus("RUNNING")
                .setUptimeMs(getUptime())
                .setActiveConnections(getActiveConnections())
                .putMetrics("active_transactions", String.valueOf(activeTransactions.size()))
                .build();
                
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            
        } catch (Exception e) {
            logger.error("Error getting server status: {}", e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL
                .withDescription("Failed to get server status: " + e.getMessage())
                .asException());
        }
    }
    
    // Helper methods
    
    private Database getDatabase(String databaseName, DatabaseCredentials credentials) {
       
    	// Validate credentials
        validateCredentials(credentials);
        
        // Use the same approach as Postgres/Redis plugins
        if (arcadeServer != null) {
            
        	// This is how other plugins do it - get the already-open database
            Database db = arcadeServer.getDatabase(databaseName);
            
            logger.debug("getDatabase(): db = {} isOpen = {}", db, db.isOpen());
            
            if (db != null) {
                return db;
            }
        }
        
        // Check if database is already in the pool
        String poolKey = databaseName;
        Database database = databasePool.get(poolKey);
        
        if (database != null && database.isOpen()) {
            // Return existing open database
            return database;
        }
        
        // Create new database connection
        synchronized (databasePool) {
            // Double-check after acquiring lock
            database = databasePool.get(poolKey);
            if (database != null && database.isOpen()) {
                return database;
            }
            
            // Create database factory for the specific database
            DatabaseFactory dbFactory = new DatabaseFactory(databasePath + "/" + databaseName);
            
            try {
                // Open database - ArcadeDB requires MODE parameter
                if (dbFactory.exists()) {
                    // Try READ_ONLY first to avoid conflicts
                    try {
                        database = dbFactory.open(ComponentFile.MODE.READ_ONLY);
                    } catch (Exception e) {
                        // If READ_ONLY fails, try READ_WRITE
                        logger.debug("Opening database in READ_WRITE mode: {}", databaseName);
                        database = dbFactory.open(ComponentFile.MODE.READ_WRITE);
                    }
                } else {
                    // Create if it doesn't exist
                    database = dbFactory.create();
                }
                
                // Add to pool
                databasePool.put(poolKey, database);
                return database;
                
            } catch (Exception e) {
                logger.error("Failed to open database: {}", databaseName, e);
                throw new RuntimeException("Cannot open database: " + databaseName + " - " + e.getMessage(), e);
            }
        }
    }
    
    private void validateCredentials(DatabaseCredentials credentials) {
        // Implement credential validation logic
        // This is a placeholder - integrate with ArcadeDB's security system
        if (credentials == null || credentials.getUsername().isEmpty()) {
            throw new IllegalArgumentException("Invalid credentials");
        }
    }
    
    private Map<String, Object> convertParameters(Map<String, Value> protoParams) {
        Map<String, Object> params = new HashMap<>();
        for (Map.Entry<String, Value> entry : protoParams.entrySet()) {
            params.put(entry.getKey(), convertFromProtobufValue(entry.getValue()));
        }
        return params;
    }
    
    private Object convertFromProtobufValue(Value value) {
        switch (value.getKindCase()) {
            case NULL_VALUE:
                return null;
            case NUMBER_VALUE:
                return value.getNumberValue();
            case STRING_VALUE:
                return value.getStringValue();
            case BOOL_VALUE:
                return value.getBoolValue();
            case STRUCT_VALUE:
                return convertFromProtobufValues(value.getStructValue().getFieldsMap());
            case LIST_VALUE:
                return value.getListValue().getValuesList().stream()
                    .map(this::convertFromProtobufValue)
                    .toArray();
            default:
                return null;
        }
    }
    
    private Map<String, Object> convertFromProtobufValues(Map<String, Value> values) {
        Map<String, Object> result = new HashMap<>();
        for (Map.Entry<String, Value> entry : values.entrySet()) {
            result.put(entry.getKey(), convertFromProtobufValue(entry.getValue()));
        }
        return result;
    }
    
    private Record convertToGrpcRecord(com.arcadedb.database.Record dbRecord) {
        Record.Builder builder = Record.newBuilder()
            .setRid(dbRecord.getIdentity().toString());
        
        // Handle different record types
        if (dbRecord instanceof Document) {
            Document doc = (Document) dbRecord;
            builder.setType(doc.getTypeName());
            
            // Convert properties
            Set<String> properties = doc.getPropertyNames();
            for (String property : properties) {
                Object value = doc.get(property);
                if (value != null) {
                    builder.putProperties(property, convertToProtobufValue(value));
                }
            }
        } else if (dbRecord instanceof Vertex) {
            Vertex vertex = (Vertex) dbRecord;
            builder.setType(vertex.getTypeName());
            
            // Convert properties
            Set<String> properties = vertex.getPropertyNames();
            for (String property : properties) {
                Object value = vertex.get(property);
                if (value != null) {
                    builder.putProperties(property, convertToProtobufValue(value));
                }
            }
        } else if (dbRecord instanceof Edge) {
            Edge edge = (Edge) dbRecord;
            builder.setType(edge.getTypeName());
            
            // Convert properties
            Set<String> properties = edge.getPropertyNames();
            for (String property : properties) {
                Object value = edge.get(property);
                if (value != null) {
                    builder.putProperties(property, convertToProtobufValue(value));
                }
            }
        }
        
        return builder.build();
    }
    
    private Value convertToProtobufValue(Object value) {
        if (value == null) {
            return Value.newBuilder().setNullValue(com.google.protobuf.NullValue.NULL_VALUE).build();
        } else if (value instanceof Boolean) {
            return Value.newBuilder().setBoolValue((Boolean) value).build();
        } else if (value instanceof Number) {
            return Value.newBuilder().setNumberValue(((Number) value).doubleValue()).build();
        } else if (value instanceof String) {
            return Value.newBuilder().setStringValue((String) value).build();
        } else {
            // For complex objects, convert to string
            return Value.newBuilder().setStringValue(value.toString()).build();
        }
    }
    
    private String generateTransactionId() {
        return "tx_" + System.nanoTime();
    }
    
    private long getUptime() {
        // Implement uptime calculation
        return System.currentTimeMillis();
    }
    
    private int getActiveConnections() {
        // Implement active connections count
        return 0;
    }
}