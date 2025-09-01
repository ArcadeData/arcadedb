package com.arcadedb.server.grpc;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.grpc.InsertOptions.ConflictMode;
import com.arcadedb.server.grpc.InsertOptions.TransactionMode;
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

	private static final int DEFAULT_MAX_COMMAND_ROWS = 1000;

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
			}
			catch (Exception e) {
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
			}
			catch (Exception e) {
				logger.error("Error closing transaction database", e);
			}
		}
		activeTransactions.clear();
	}

	@Override
	public void executeCommand(ExecuteCommandRequest req, StreamObserver<ExecuteCommandResponse> resp) {
		final long t0 = System.nanoTime();

		Database db = null;
		boolean beganHere = false;

		try {
			// Resolve DB + params
			db = getDatabase(req.getDatabase(), req.getCredentials());
			final java.util.Map<String, Object> params = convertParameters(req.getParametersMap());

			// Language defaults to "sql" when empty
			final String language = (req.getLanguage() == null || req.getLanguage().isEmpty()) ? "sql" : req.getLanguage();

			// Transaction: begin if requested
			final boolean hasTx = req.hasTransaction();
			final var tx = hasTx ? req.getTransaction() : null;
			if (hasTx && tx.getBegin()) {
				db.begin();
				beganHere = true;
			}

			long affected = 0L;
			final boolean returnRows = req.getReturnRows();
			final int maxRows = req.getMaxRows() > 0 ? req.getMaxRows() : DEFAULT_MAX_COMMAND_ROWS;

			ExecuteCommandResponse.Builder out = ExecuteCommandResponse.newBuilder().setSuccess(true).setMessage("OK");

			// Execute the command
			try (ResultSet rs = db.command(language, req.getCommand(), params)) {
				if (returnRows) {
					int emitted = 0;
					while (rs.hasNext()) {
						Result r = rs.next();

						if (r.isElement()) {
							affected++; // count modified/returned records
							if (emitted < maxRows) {
								out.addRecords(convertToGrpcRecord(r.getElement().get()));
								emitted++;
							}
						}
						else {
							// Scalar / projection row (e.g., RETURN COUNT)
							if (emitted < maxRows) {
								com.arcadedb.server.grpc.Record.Builder recB = com.arcadedb.server.grpc.Record.newBuilder();
								for (String p : r.getPropertyNames()) {
									recB.putProperties(p, toProtoValue(r.getProperty(p)));
								}
								out.addRecords(recB.build());
								emitted++;
							}
							for (String p : r.getPropertyNames()) {
								Object v = r.getProperty(p);
								if (v instanceof Number n)
									affected += n.longValue();
							}
						}
					}
				}
				else {
					// Not returning rows: still consume to compute 'affected'
					while (rs.hasNext()) {
						Result r = rs.next();
						if (r.isElement()) {
							affected++;
						}
						else {
							for (String p : r.getPropertyNames()) {
								Object v = r.getProperty(p);
								if (v instanceof Number n)
									affected += n.longValue();
							}
						}
					}
				}
			}

			// Transaction end — precedence: rollback > commit > begin-only⇒commit
			if (hasTx) {
				if (tx.getRollback()) {
					db.rollback();
				}
				else if (tx.getCommit()) {
					db.commit();
				}
				else if (beganHere) {
					// Began but no explicit commit/rollback flag — default to commit (HTTP parity)
					db.commit();
				}
			}

			final long ms = (System.nanoTime() - t0) / 1_000_000L;
			out.setAffectedRecords(affected).setExecutionTimeMs(ms);

			resp.onNext(out.build());
			resp.onCompleted();

		}
		catch (Exception e) {
			// Best-effort rollback if we began here and failed
			try {
				if (beganHere && db != null)
					db.rollback();
			}
			catch (Exception ignore) {
				/* no-op */ }

			final long ms = (System.nanoTime() - t0) / 1_000_000L;
			ExecuteCommandResponse err = ExecuteCommandResponse.newBuilder().setSuccess(false)
					.setMessage(e.getMessage() == null ? e.toString() : e.getMessage()).setAffectedRecords(0L).setExecutionTimeMs(ms).build();

			// Prefer returning a structured response so clients always get timing/message
			resp.onNext(err);
			resp.onCompleted();
			// If you prefer gRPC error codes: comment the two lines above and use:
			// resp.onError(Status.INTERNAL.withDescription("ExecuteCommand: " +
			// e.getMessage()).asException());
		}
	}

	@Override
	public void createRecord(CreateRecordRequest req, StreamObserver<CreateRecordResponse> resp) {

		try {

			Database db = getDatabase(req.getDatabase(), req.getCredentials());

			final String cls = req.getType(); // or req.getTargetClass() if that’s your proto
			if (cls == null || cls.isEmpty())
				throw new IllegalArgumentException("targetClass is required");

			Schema schema = db.getSchema();
			DocumentType dt = schema.getType(cls);
			if (dt == null)
				throw new IllegalArgumentException("Class not found: " + cls);

			// All properties from the request (proto map) — nested under "record"
			final java.util.Map<String, com.google.protobuf.Value> props = req.getRecord().getPropertiesMap();

			// --- Vertex ---
			if (dt instanceof com.arcadedb.schema.VertexType) {
				com.arcadedb.graph.MutableVertex v = db.newVertex(cls);

				// apply properties with proper coercion
				props.forEach((k, val) -> v.set(k, toJavaForProperty(db, dt, k, val)));

				v.save();

				// Build response: proto has setRid(String)
				CreateRecordResponse.Builder b = CreateRecordResponse.newBuilder().setRid(v.getIdentity().toString());

				resp.onNext(b.build());
				resp.onCompleted();
				return;
			}

			// --- Edge ---
			if (dt instanceof EdgeType) {
				// Expect 'out' and 'in' as string RIDs in the properties map
				String outStr = null, inStr = null;
				if (props.containsKey("out")) {
					var pv = props.get("out");
					outStr = pv.hasStringValue() ? pv.getStringValue() : String.valueOf(toJava(pv));
				}
				if (props.containsKey("in")) {
					var pv = props.get("in");
					inStr = pv.hasStringValue() ? pv.getStringValue() : String.valueOf(toJava(pv));
				}
				if (outStr == null || inStr == null)
					throw new IllegalArgumentException("Edge requires 'out' and 'in' RIDs");

				var outEl = db.lookupByRID(new RID(outStr), true);
				var inEl = db.lookupByRID(new RID(inStr), true);
				if (outEl == null || inEl == null)
					throw new IllegalArgumentException("Cannot resolve out/in vertices for edge");

				// IMPORTANT: use MutableVertex to avoid newEdge(...) ambiguity
				com.arcadedb.graph.MutableVertex outV = (MutableVertex) outEl.asVertex(true);
				com.arcadedb.graph.MutableVertex inV = (MutableVertex) inEl.asVertex(true);

				com.arcadedb.graph.MutableEdge e = outV.newEdge(cls, inV);

				// apply remaining properties (skip endpoints)
				props.forEach((k, val) -> {
					if (!"out".equals(k) && !"in".equals(k)) {
						e.set(k, toJavaForProperty(db, dt, k, val));
					}
				});

				e.save();

				CreateRecordResponse.Builder b = CreateRecordResponse.newBuilder();

				b.setRid(e.getIdentity().toString());

				resp.onNext(b.build());
				resp.onCompleted();

				return;
			}

			// --- Document ---
			MutableDocument d = db.newDocument(cls);
			props.forEach((k, val) -> d.set(k, toJavaForProperty(db, dt, k, val)));
			d.save();

			CreateRecordResponse.Builder b = CreateRecordResponse.newBuilder();

			b.setRid(d.getIdentity().toString());

			resp.onNext(b.build());
			resp.onCompleted();

		}
		catch (Exception e) {
			resp.onError(Status.INVALID_ARGUMENT.withDescription("CreateRecord: " + e.getMessage()).asException());
		}
	}

	@Override
	public void lookupByRid(LookupByRidRequest req, StreamObserver<LookupByRidResponse> resp) {
		try {
			Database db = getDatabase(req.getDatabase(), req.getCredentials());

			final String ridStr = req.getRid();
			if (ridStr == null || ridStr.isBlank())
				throw new IllegalArgumentException("rid is required");

			var el = db.lookupByRID(new RID(ridStr), true);
			if (el == null) {
				resp.onNext(LookupByRidResponse.newBuilder().setFound(false).build());
				resp.onCompleted();
				return;
			}

			resp.onNext(LookupByRidResponse.newBuilder().setFound(true).setRecord(convertToGrpcRecord(el.getRecord())).build());
			resp.onCompleted();
		}
		catch (Exception e) {
			resp.onError(Status.INTERNAL.withDescription("LookupByRid: " + e.getMessage()).asException());
		}
	}

	@Override
	public void updateRecord(UpdateRecordRequest req, StreamObserver<UpdateRecordResponse> resp) {

		Database db = null;
		boolean beganHere = false;

		try {

			db = getDatabase(req.getDatabase(), req.getCredentials());

			final String ridStr = req.getRid();
			if (ridStr == null || ridStr.isEmpty())
				throw new IllegalArgumentException("rid is required");

			// Begin transaction if requested
			final boolean hasTx = req.hasTransaction();
			final var tx = hasTx ? req.getTransaction() : null;
			if (hasTx && tx.getBegin()) {
				db.begin();
				beganHere = true;
			}

			// Lookup the record by RID
			var el = db.lookupByRID(new RID(ridStr), true);
			if (el == null) {
				resp.onError(Status.NOT_FOUND.withDescription("Record not found: " + ridStr).asException());
				return;
			}

			// Get mutable view for updates (works for docs, vertices, edges)
			MutableDocument mdoc = (MutableDocument) el.asDocument(true);

			var dtype = db.getSchema().getType(mdoc.getTypeName());

			final var dbRef = db;

			// Apply updates

			final java.util.Map<String, com.google.protobuf.Value> props = req.hasRecord() ? req.getRecord().getPropertiesMap()
					: req.hasPartial() ? req.getPartial().getPropertiesMap() : java.util.Collections.emptyMap();

			// apply updates
			props.forEach((k, v) -> mdoc.set(k, toJavaForProperty(dbRef, dtype, k, v)));

			mdoc.save();

			// Commit/rollback with proper precedence
			if (hasTx) {
				if (tx.getRollback()) {
					db.rollback();
				}
				else if (tx.getCommit()) {
					db.commit();
				}
				else if (beganHere) {
					db.commit();
				}
			}

			resp.onNext(UpdateRecordResponse.newBuilder().setUpdated(true).setSuccess(true).build());
			resp.onCompleted();

		}
		catch (Exception e) {
			try {
				if (beganHere && db != null)
					db.rollback();
			}
			catch (Exception ignore) {
				// ignore rollback errors
			}
			resp.onError(Status.INTERNAL.withDescription("UpdateRecord: " + e.getMessage()).asException());
		}
	}

	@Override
	public void deleteRecord(DeleteRecordRequest req, StreamObserver<DeleteRecordResponse> resp) {
		Database db = null;
		boolean beganHere = false;

		try {
			db = getDatabase(req.getDatabase(), req.getCredentials());

			final String ridStr = req.getRid();
			if (ridStr == null || ridStr.isBlank())
				throw new IllegalArgumentException("rid is required");

			// Optional tx begin
			if (req.hasTransaction() && req.getTransaction().getBegin()) {
				db.begin();
				beganHere = true;
			}

			var el = db.lookupByRID(new RID(ridStr), true);
			if (el == null) {
				// Soft "not found"
				resp.onNext(
						DeleteRecordResponse.newBuilder().setSuccess(true).setDeleted(false).setMessage("Record not found: " + ridStr).build());
				resp.onCompleted();
				// If we began here and no explicit rollback flag, default to commit (or
				// rollback—your policy)
				if (beganHere && req.hasTransaction() && !req.getTransaction().getRollback())
					db.commit();
				return;
			}

			el.delete(); // deletes document/vertex/edge

			// Optional tx end controls
			if (req.hasTransaction()) {
				var tx = req.getTransaction();
				if (tx.getCommit())
					db.commit();
				else if (tx.getRollback())
					db.rollback();
				else if (beganHere)
					db.commit(); // began here with no explicit end → commit by default
			}
			else if (beganHere) {
				db.commit();
			}

			resp.onNext(DeleteRecordResponse.newBuilder().setSuccess(true).setDeleted(true).build());
			resp.onCompleted();

		}
		catch (Exception e) {
			// Best-effort rollback if we started the tx
			try {
				if (beganHere && db != null)
					db.rollback();
			}
			catch (Exception ignore) {
			}

			resp.onError(
					Status.INTERNAL.withDescription("DeleteRecord: " + (e.getMessage() == null ? e.toString() : e.getMessage())).asException());
		}
	}

	@Override
	public void executeQuery(ExecuteQueryRequest request, StreamObserver<ExecuteQueryResponse> responseObserver) {
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
			ResultSet resultSet = database.query("sql", request.getQuery(), convertParameters(request.getParametersMap()));

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

			ExecuteQueryResponse response = ExecuteQueryResponse.newBuilder().addResults(resultBuilder.build()).setExecutionTimeMs(executionTime)
					.build();

			logger.debug("executeQuery(): executionTime + response generation = {}", executionTime);

			responseObserver.onNext(response);
			responseObserver.onCompleted();

		}
		catch (Exception e) {
			logger.error("Error executing query: {}", e.getMessage(), e);
			responseObserver.onError(Status.INTERNAL.withDescription("Query execution failed: " + e.getMessage()).asException());
		}
	}

	@Override
	public void beginTransaction(BeginTransactionRequest request, StreamObserver<BeginTransactionResponse> responseObserver) {
		try {
			Database database = getDatabase(request.getDatabase(), request.getCredentials());

			// Begin transaction
			database.begin();

			// Generate transaction ID
			String transactionId = generateTransactionId();
			activeTransactions.put(transactionId, database);

			BeginTransactionResponse response = BeginTransactionResponse.newBuilder().setTransactionId(transactionId)
					.setTimestamp(System.currentTimeMillis()).build();

			responseObserver.onNext(response);
			responseObserver.onCompleted();

		}
		catch (Exception e) {
			logger.error("Error beginning transaction: {}", e.getMessage(), e);
			responseObserver.onError(Status.INTERNAL.withDescription("Failed to begin transaction: " + e.getMessage()).asException());
		}
	}

	@Override
	public void commitTransaction(CommitTransactionRequest request, StreamObserver<CommitTransactionResponse> responseObserver) {
		try {
			Database database = activeTransactions.remove(request.getTransaction().getTransactionId());
			if (database == null) {
				throw new IllegalArgumentException("Invalid transaction ID");
			}

			// Commit transaction
			database.commit();

			CommitTransactionResponse response = CommitTransactionResponse.newBuilder().setSuccess(true)
					.setMessage("Transaction committed successfully").setTimestamp(System.currentTimeMillis()).build();

			responseObserver.onNext(response);
			responseObserver.onCompleted();

		}
		catch (Exception e) {
			logger.error("Error committing transaction: {}", e.getMessage(), e);
			responseObserver.onError(Status.INTERNAL.withDescription("Failed to commit transaction: " + e.getMessage()).asException());
		}
	}

	@Override
	public void rollbackTransaction(RollbackTransactionRequest request, StreamObserver<RollbackTransactionResponse> responseObserver) {
		try {
			Database database = activeTransactions.remove(request.getTransaction().getTransactionId());
			if (database == null) {
				throw new IllegalArgumentException("Invalid transaction ID");
			}

			// Rollback transaction
			database.rollback();

			RollbackTransactionResponse response = RollbackTransactionResponse.newBuilder().setSuccess(true)
					.setMessage("Transaction rolled back successfully").build();

			responseObserver.onNext(response);
			responseObserver.onCompleted();

		}
		catch (Exception e) {
			logger.error("Error rolling back transaction: {}", e.getMessage(), e);
			responseObserver.onError(Status.INTERNAL.withDescription("Failed to rollback transaction: " + e.getMessage()).asException());
		}
	}

	@Override
	public void streamQuery(StreamQueryRequest request, StreamObserver<QueryResult> responseObserver) {

		final ServerCallStreamObserver<QueryResult> scso = (ServerCallStreamObserver<QueryResult>) responseObserver;

		final java.util.concurrent.atomic.AtomicBoolean cancelled = new java.util.concurrent.atomic.AtomicBoolean(false);
		scso.setOnCancelHandler(() -> cancelled.set(true));

		Database db = null;
		boolean beganHere = false;

		try {
			db = getDatabase(request.getDatabase(), request.getCredentials());
			final int batchSize = Math.max(1, request.getBatchSize());

			// --- TX begin if requested ---
			final boolean hasTx = request.hasTransaction();
			final var tx = hasTx ? request.getTransaction() : null;
			if (hasTx && tx.getBegin()) {
				db.begin();
				beganHere = true;
			}

			// --- Dispatch on mode (helpers do NOT manage transactions) ---
			switch (request.getRetrievalMode()) {
			case MATERIALIZE_ALL -> streamMaterialized(db, request, batchSize, scso, cancelled);
			case PAGED -> streamPaged(db, request, batchSize, scso, cancelled);
			case CURSOR -> streamCursor(db, request, batchSize, scso, cancelled);
			default -> streamCursor(db, request, batchSize, scso, cancelled);
			}

			// If the client cancelled mid-stream, choose rollback unless caller explicitly
			// asked to commit/rollback.
			if (cancelled.get()) {
				if (hasTx) {
					if (tx.getRollback()) {
						db.rollback();
					}
					else if (tx.getCommit()) {
						db.commit(); // caller explicitly wanted commit even if they cancelled
					}
					else if (beganHere) {
						db.rollback(); // safe default on cancellation
					}
				}
				return; // don't call onCompleted()
			}

			// --- TX end (normal path) — precedence: rollback > commit > begin-only ⇒
			// commit ---
			if (hasTx) {
				if (tx.getRollback()) {
					db.rollback();
				}
				else if (tx.getCommit()) {
					db.commit();
				}
				else if (beganHere) {
					db.commit();
				}
			}

			scso.onCompleted();

		}
		catch (Exception e) {
			// Best-effort rollback only if we began here and there wasn't an explicit
			// commit/rollback
			try {
				if (beganHere && db != null) {
					if (request.hasTransaction()) {
						var tx = request.getTransaction();
						if (!tx.getCommit() && !tx.getRollback())
							db.rollback();
					}
					else {
						db.rollback();
					}
				}
			}
			catch (Exception ignore) {
				/* no-op */ }

			if (!cancelled.get()) {
				responseObserver.onError(Status.INTERNAL.withDescription("Stream query failed: " + e.getMessage()).asException());
			}
		}
	}

	/**
	 * Mode 1 (existing behavior-ish): run once and iterate results, batching as we
	 * go.
	 */
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
		// Skip if you’re okay with best-effort pushes; otherwise honor transport
		// readiness
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

		final InsertOptions opts = defaults(req.getOptions()); // apply defaults (batch size, tx mode, etc.)
		final long started = System.currentTimeMillis();

		try (InsertContext ctx = new InsertContext(opts)) {

			Counts totals = insertRows(ctx, req.getRowsList().iterator());

			ctx.flushCommit(true);

			resp.onNext(ctx.summary(totals, started));
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

		final long startedAt = System.currentTimeMillis();

		final java.util.concurrent.atomic.AtomicBoolean cancelled = new java.util.concurrent.atomic.AtomicBoolean(false);
		final java.util.concurrent.atomic.AtomicReference<InsertContext> ctxRef = new java.util.concurrent.atomic.AtomicReference<>();

		final Counts totals = new Counts();

		// cache the first-chunk effective options to validate consistency
		final java.util.concurrent.atomic.AtomicReference<InsertOptions> firstOptsRef = new java.util.concurrent.atomic.AtomicReference<>();

		call.setOnCancelHandler(() -> cancelled.set(true));

		// Pull the very first inbound message
		call.request(1);

		return new StreamObserver<>() {

			@Override
			public void onNext(InsertChunk c) {

				if (cancelled.get()) {
					return;
				}

				try {

					InsertContext ctx = ctxRef.get();

					if (ctx == null) {

						// -------- First chunk: derive effective options and build context --------

						// Options may be provided on the first chunk; otherwise use server defaults
						InsertOptions sent = c.hasOptions() ? c.getOptions() : InsertOptions.getDefaultInstance();

						InsertOptions effective = defaults(sent); // your existing default-merging helper

						// (Optional) If your chunk carries db/creds (recommended), you can validate/set
						// them here

						// Otherwise InsertContext will resolve them via 'effective' or server defaults
						// Example if your proto carries these on the chunk:
						// String dbName = c.getDatabase();
						// DatabaseCredentials creds = c.getCredentials();
						// and pass them to the InsertContext ctor if it expects them.

						// Create and cache context
						ctx = new InsertContext(effective); // begins tx if PER_STREAM / PER_BATCH per your logic

						ctxRef.set(ctx);

						firstOptsRef.set(effective);

						if (logger.isDebugEnabled())
							logger.debug("insertStream: initialized context with options:\n{}", effective);
					}
					else {

						// -------- Subsequent chunks: optionally validate option consistency --------
						if (c.hasOptions()) {
							InsertOptions prev = firstOptsRef.get();
							InsertOptions cur = defaults(c.getOptions());
							// Minimal consistency check (customize as you wish)
							if (!sameInsertContract(prev, cur)) {
								// contract changed mid-stream -> fail fast
								throw new IllegalArgumentException("insertStream: options changed after first chunk");
							}
						}
					}

					// -------- Insert rows for this chunk --------
					Counts cts = insertRows(ctxRef.get(), c.getRowsList().iterator());
					totals.add(cts);
				}
				catch (Exception e) {
					// Register as an error on totals; keep stream alive unless you prefer to fail
					// the call
					totals.err(Math.max(0, (ctxRef.get() != null ? ctxRef.get().received : 0) - 1), "DB_ERROR", e.getMessage(), "");
				}
				finally {
					if (!cancelled.get())
						call.request(1);
				}
			}

			@Override
			public void onError(Throwable t) {
				InsertContext ctx = ctxRef.get();
				if (ctx != null)
					ctx.closeQuietly();
			}

			@Override
			public void onCompleted() {
				try {

					InsertContext ctx = ctxRef.get();
					if (ctx == null) {
						// Client closed without sending a first chunk → nothing to do
						resp.onNext(InsertSummary.newBuilder().setReceived(0).setInserted(0).setUpdated(0).setIgnored(0).setFailed(0)
								.setExecutionTimeMs(System.currentTimeMillis() - startedAt).build());

						resp.onCompleted();
						return;
					}

					ctx.flushCommit(true); // commit if not validate-only
					if (!cancelled.get()) {
						resp.onNext(ctx.summary(totals, startedAt));
						resp.onCompleted();
					}
				}
				catch (Exception e) {
					resp.onError(Status.INTERNAL.withDescription("insertStream: " + e.getMessage()).asException());
				}
				finally {
					InsertContext ctx = ctxRef.get();
					if (ctx != null)
						ctx.closeQuietly();
				}
			}
		};
	}

	/** Minimal consistency check: same "contract" for the stream. */
	private boolean sameInsertContract(InsertOptions a, InsertOptions b) {
		if (a == null || b == null)
			return a == b;
		// lock down the knobs that must not change mid-stream
		if (a.getTargetClass() != null && !a.getTargetClass().equals(b.getTargetClass()))
			return false;
		if (a.getConflictMode() != b.getConflictMode())
			return false;
		if (a.getTransactionMode() != b.getTransactionMode())
			return false;
		// You can also compare key/update columns if you require strict equality:
		if (!a.getKeyColumnsList().equals(b.getKeyColumnsList()))
			return false;
		if (!a.getUpdateColumnsOnConflictList().equals(b.getUpdateColumnsOnConflictList()))
			return false;
		return true;
	}

	@Override
	public StreamObserver<InsertRequest> insertBidirectional(StreamObserver<InsertResponse> resp) {
		final ServerCallStreamObserver<InsertResponse> call = (ServerCallStreamObserver<InsertResponse>) resp;
		call.disableAutoInboundFlowControl();

		final java.util.concurrent.atomic.AtomicReference<InsertContext> ref = new java.util.concurrent.atomic.AtomicReference<>();
		final java.util.concurrent.atomic.AtomicBoolean cancelled = new java.util.concurrent.atomic.AtomicBoolean(false);
		final java.util.concurrent.atomic.AtomicBoolean started = new java.util.concurrent.atomic.AtomicBoolean(false);
		
		final java.util.concurrent.ConcurrentLinkedQueue<InsertResponse> outQueue = new java.util.concurrent.ConcurrentLinkedQueue<>();

		// drain helper
		final Runnable drain = () -> {
			while (call.isReady()) {
				InsertResponse next = outQueue.poll();
				if (next == null)
					break;
				resp.onNext(next);
			}
		};

		// enqueue or send immediately
		final java.util.function.Consumer<InsertResponse> sendOrQueue = (ir) -> {
			outQueue.offer(ir);
			drain.run();
		};		

		call.setOnReadyHandler(drain);
		
		call.setOnCancelHandler(() -> {
			cancelled.set(true);
			final InsertContext ctx = ref.getAndSet(null);
			if (ctx != null) {
				sessionWatermark.remove(ctx.sessionId);
				ctx.closeQuietly();
			}
		});

		call.request(1);

		return new StreamObserver<>() {
			@Override
			public void onNext(InsertRequest reqMsg) {
				if (cancelled.get())
					return;

				try {
					switch (reqMsg.getMsgCase()) {

					case START -> {
						if (!started.compareAndSet(false, true)) {
							resp.onError(
									Status.FAILED_PRECONDITION.withDescription("insertBidirectional: START already received").asException());
							return;
						}

						final InsertOptions opts = defaults(reqMsg.getStart().getOptions());
						final InsertContext ctx = new InsertContext(opts);
						ctx.startedAt = System.currentTimeMillis();
						ctx.totals = new Counts();

						ref.set(ctx);
						sessionWatermark.put(ctx.sessionId, 0L);
						
						sendOrQueue.accept(
							InsertResponse.newBuilder()
							    .setStarted(Started.newBuilder().setSessionId(ctx.sessionId).build())
							    .build());

						// pull next message
						call.request(1);
					}

					case CHUNK -> {
						final InsertContext ctx = require(ref.get(), "session not started");
						final InsertChunk c = reqMsg.getChunk();

						// idempotent replay guard
						final long hi = sessionWatermark.getOrDefault(ctx.sessionId, 0L);
						
						if (c.getChunkSeq() <= hi) {
							
							resp.onNext(InsertResponse.newBuilder().setBatchAck(BatchAck.newBuilder().setSessionId(ctx.sessionId)
									.setChunkSeq(c.getChunkSeq()).setInserted(0).setUpdated(0).setIgnored(0).setFailed(0).build()).build());
							
							call.request(1);
							return;
						}

						Counts perChunk;
						try {
							perChunk = insertRows(ctx, c.getRowsList().iterator());
							ctx.totals.add(perChunk);
							sessionWatermark.put(ctx.sessionId, c.getChunkSeq());
							
							sendOrQueue.accept(InsertResponse.newBuilder()
								    .setBatchAck(BatchAck.newBuilder()
								        .setSessionId(ctx.sessionId).setChunkSeq(c.getChunkSeq())
								        .setInserted(perChunk.inserted).setUpdated(perChunk.updated)
								        .setIgnored(perChunk.ignored).setFailed(perChunk.failed)
								        .addAllErrors(perChunk.errors)
								        .build())
								    .build());
							
							
						}
						catch (Exception e) {
							// surface as failed chunk and continue (or switch to resp.onError(...) if you
							// want to fail fast)
							
							perChunk = new Counts();
							
							perChunk.failed = c.getRowsCount();
							
							perChunk.errors.add(InsertError.newBuilder().setRowIndex(Math.max(0, ctx.received - 1)).setCode("DB_ERROR")
									.setMessage(String.valueOf(e.getMessage())).build());
							ctx.totals.add(perChunk);
							// intentionally do not advance watermark on failure; client may replay safely
						}

//						resp.onNext(InsertResponse.newBuilder()
//								.setBatchAck(BatchAck.newBuilder().setSessionId(ctx.sessionId).setChunkSeq(c.getChunkSeq())
//										.setInserted(perChunk.inserted).setUpdated(perChunk.updated).setIgnored(perChunk.ignored)
//										.setFailed(perChunk.failed).addAllErrors(perChunk.errors).build())
//								.build());

						sendOrQueue.accept(
							     InsertResponse.newBuilder()
									.setBatchAck(BatchAck.newBuilder()
									.setSessionId(ctx.sessionId)
									.setChunkSeq(c.getChunkSeq())
									.setInserted(perChunk.inserted)
									.setUpdated(perChunk.updated)
									.setIgnored(perChunk.ignored)
									.setFailed(perChunk.failed)
									.addAllErrors(perChunk.errors)
									.build())
								.build());							
							
						call.request(1);
					}

					case COMMIT -> {
						final InsertContext ctx = require(ref.get(), "session not started");
						try {
							ctx.flushCommit(true); // commit unless validate_only in your InsertContext logic
							final InsertSummary sum = ctx.summary(ctx.totals, ctx.startedAt);
							resp.onNext(InsertResponse.newBuilder().setCommitted(Committed.newBuilder().setSummary(sum).build()).build());
							resp.onCompleted();
						}
						catch (Exception e) {
							resp.onError(Status.INTERNAL.withDescription("commit: " + e.getMessage()).asException());
						}
						finally {
							sessionWatermark.remove(ctx.sessionId);
							ctx.closeQuietly();
							ref.set(null);
						}
					}

					case MSG_NOT_SET -> {
						// ignore
						call.request(1);
					}
					}
				}
				catch (Exception unexpected) {
					// defensive: fail fast on unexpected exceptions
					resp.onError(Status.INTERNAL.withDescription("insertBidirectional: " + unexpected.getMessage()).asException());
					final InsertContext ctx = ref.getAndSet(null);
					if (ctx != null) {
						sessionWatermark.remove(ctx.sessionId);
						ctx.closeQuietly();
					}
				}
			}

			@Override
			public void onError(Throwable t) {
				final InsertContext ctx = ref.getAndSet(null);
				if (ctx != null) {
					sessionWatermark.remove(ctx.sessionId);
					ctx.closeQuietly();
				}
			}

			@Override
			public void onCompleted() {
				// Define your policy for half-close without COMMIT:
				final InsertContext ctx = ref.getAndSet(null);
				if (ctx != null) {
					try {
						// Safer default is rollback; if you prefer auto-commit on close, call
						// flushCommit(true)
						ctx.flushCommit(false); // or ctx.rollbackQuietly() if you have a helper
					}
					catch (Exception ignore) {
					}
					sessionWatermark.remove(ctx.sessionId);
					ctx.closeQuietly();
				}
			}
		};
	}

	private boolean tryUpsertVertex(InsertContext ctx, com.arcadedb.graph.MutableVertex incoming) {
		
		var keys = ctx.keyCols;
		
		if (keys.isEmpty())
			return false;

		String where = String.join(" AND ", keys.stream().map(k -> k + " = ?").toList());
		
		Object[] params = keys.stream()
				// read key values from the incoming vertex as a document
				.map(k -> ((MutableDocument) incoming).get(k)).toArray();

		try (var rs = ctx.db.query("sql", "SELECT FROM " + ctx.opts.getTargetClass() + " WHERE " + where, params)) {
			if (!rs.hasNext())
				return false;
			var res = rs.next();
			if (!res.isElement())
				return false;

			// mutate via MutableDocument view (valid for vertex records)
			MutableVertex existingDoc = (MutableVertex) res.getElement().get().asDocument(true);
			for (String col : ctx.updateCols) {
				existingDoc.set(col, ((MutableDocument) incoming).get(col));
			}
			existingDoc.save();
			return true;
		}
	}

	private boolean tryUpsertDocument(InsertContext ctx, MutableDocument incoming) {
		var keys = ctx.keyCols;
		if (keys.isEmpty())
			return false;

		String where = String.join(" AND ", keys.stream().map(k -> k + " = ?").toList());
		Object[] params = keys.stream().map(incoming::get).toArray();

		try (var rs = ctx.db.query("sql", "SELECT FROM " + ctx.opts.getTargetClass() + " WHERE " + where, params)) {
			if (!rs.hasNext())
				return false;
			var res = rs.next();
			if (!res.isElement())
				return false;

			var existing = (MutableDocument) res.getElement().get().asDocument(true);
			for (String col : ctx.updateCols)
				existing.set(col, incoming.get(col));
			existing.save();
			return true;
		}
	}

	// ---------- Core insert plumbing ----------

	private static final class Counts {

		long received, inserted, updated, ignored, failed;

		final java.util.List<InsertError> errors = new java.util.ArrayList<>();

		void add(Counts o) {
			received += o.received;
			inserted += o.inserted;
			updated += o.updated;
			ignored += o.ignored;
			failed += o.failed;
			errors.addAll(o.errors);
		}

		void err(long rowIndex, String code, String msg, String field) {
			failed++;
			errors.add(InsertError.newBuilder().setRowIndex(rowIndex).setCode(code).setMessage(msg).setField(field).build());
		}
	}

	private InsertSummary toSummary(Counts c, long startedAtMs) {

		long now = System.currentTimeMillis();

		return InsertSummary.newBuilder().setReceived(c.received).setInserted(c.inserted).setUpdated(c.updated).setIgnored(c.ignored)
				.setFailed(c.failed).addAllErrors(c.errors).setStartedAt(ts(startedAtMs)).setFinishedAt(ts(now)).build();
	}

	private Counts insertRows(InsertContext ctx, java.util.Iterator<Record> it) {
		Counts c = new Counts();
		int inBatch = 0;

		Schema schema = ctx.db.getSchema();
		DocumentType dt = schema.getType(ctx.opts.getTargetClass());
		boolean isVertex = dt instanceof VertexType;
		boolean isEdge = dt instanceof EdgeType;

		while (it.hasNext()) {

			Record r = it.next();
			c.received++;
			ctx.received++;

			try {
				if (ctx.opts.getValidateOnly())
					continue;

				if (isVertex) {
					MutableVertex v = ctx.db.newVertex(ctx.opts.getTargetClass());
					applyGrpcRecord(v, r);
					if (ctx.opts.getConflictMode() == ConflictMode.CONFLICT_UPDATE && tryUpsertVertex(ctx, v)) {
						c.updated++;
					}
					else {
						v.save();
						c.inserted++;
					}
				}
				else if (isEdge) {

					String outRid = getStringProp(r, "out"); // lookup helper you already have
					String inRid = getStringProp(r, "in");

					if (outRid == null || inRid == null) {

						c.failed++;

						c.errors.add(InsertError.newBuilder().setRowIndex(ctx.received - 1).setCode("MISSING_ENDPOINTS")
								.setMessage("Edge requires 'out' and 'in'").build());
					}
					else {
						var outV = ctx.db.lookupByRID(new com.arcadedb.database.RID(outRid), true).asVertex(true);
						var inV = ctx.db.lookupByRID(new com.arcadedb.database.RID(inRid), true).asVertex(true);

						// Create edge from the OUT vertex
						com.arcadedb.graph.MutableEdge e = outV.newEdge(ctx.opts.getTargetClass(), inV);
						applyGrpcRecord(e, r); // sets edge properties
						e.save();
						c.inserted++;
					}
				}
				else {
					MutableDocument d = ctx.db.newDocument(ctx.opts.getTargetClass());
					applyGrpcRecord(d, r);
					if (ctx.opts.getConflictMode() == ConflictMode.CONFLICT_UPDATE && tryUpsertDocument(ctx, d)) {
						c.updated++;
					}
					else {
						d.save();
						c.inserted++;
					}
				}

			}
			catch (com.arcadedb.exception.DuplicatedKeyException dup) {
				switch (ctx.opts.getConflictMode()) {
				case CONFLICT_IGNORE -> c.ignored++;
				case CONFLICT_ABORT, UNRECOGNIZED -> c.err(ctx.received - 1, "CONFLICT", dup.getMessage(), "");
				case CONFLICT_UPDATE -> c.err(ctx.received - 1, "CONFLICT", dup.getMessage(), "");
				}
			}
			catch (Exception e) {
				c.err(ctx.received - 1, "DB_ERROR", e.getMessage(), "");
			}

			inBatch++;
			if (ctx.opts.getTransactionMode() == TransactionMode.PER_BATCH && inBatch >= serverBatchSize(ctx)) {
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

	/**
	 * Safely extract a String property from a gRPC Record.
	 *
	 * @param r   the gRPC record
	 * @param key the property key to lookup
	 * @return the string value, or null if not present
	 */
	private static String getStringProp(com.arcadedb.server.grpc.Record r, String key) {
		if (r == null || key == null || key.isEmpty()) {
			return null;
		}
		// Adjust to your proto structure:
		// If you use getPropertiesMap():
		if (r.getPropertiesMap().containsKey(key)) {
			var v = r.getPropertiesMap().get(key);
			if (v.hasStringValue()) {
				return v.getStringValue();
			}
			return v.toString(); // fallback to raw string
		}

		// If your proto uses getFieldsMap() instead:
		// if (r.getFieldsMap().containsKey(key)) {
		// var v = r.getFieldsMap().get(key);
		// if (v.hasStringValue()) {
		// return v.getStringValue();
		// }
		// return v.toString();
		// }

		return null;
	}

	private int serverBatchSize(InsertContext ctx) {
		return ctx.opts.getServerBatchSize() == 0 ? 1000 : ctx.opts.getServerBatchSize();
	}

	private void applyGrpcRecord(MutableDocument doc, com.arcadedb.server.grpc.Record r) {
		r.getPropertiesMap().forEach((k, val) -> doc.set(k, toJava(val)));
	}

	private void applyGrpcRecord(MutableVertex vertex, com.arcadedb.server.grpc.Record r) {
		r.getPropertiesMap().forEach((k, val) -> vertex.set(k, toJava(val)));
	}

	private void applyGrpcRecord(MutableEdge edge, com.arcadedb.server.grpc.Record r) {
		r.getPropertiesMap().forEach((k, val) -> edge.set(k, toJava(val)));
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
		if (v == null)
			return null;
		switch (v.getKindCase()) {
		case STRING_VALUE:
			return v.getStringValue();
		case NUMBER_VALUE:
			return v.getNumberValue(); // Double
		case BOOL_VALUE:
			return v.getBoolValue();
		case NULL_VALUE:
			return null;

		case STRUCT_VALUE: {
			// Convert each nested Value -> Java recursively
			java.util.LinkedHashMap<String, Object> m = new java.util.LinkedHashMap<>();
			v.getStructValue().getFieldsMap().forEach((k, vv) -> m.put(k, toJava(vv)));
			return m; // plain Map<String,Object>
		}

		case LIST_VALUE: {
			java.util.ArrayList<Object> list = new java.util.ArrayList<>(v.getListValue().getValuesCount());
			for (com.google.protobuf.Value item : v.getListValue().getValuesList()) {
				list.add(toJava(item));
			}
			return list;
		}

		case KIND_NOT_SET:
		default:
			return null;
		}
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

		protected Counts totals;

		final InsertOptions opts;

		final Database db;

		final java.util.List<String> keyCols;
		final java.util.List<String> updateCols;

		long startedAt;

		final String sessionId = java.util.UUID.randomUUID().toString();
		long received = 0;

		InsertContext(InsertOptions opts) {

			this.opts = opts;

			this.db = getDatabase(opts.getDatabase(), opts.getCredentials());

			this.keyCols = opts.getKeyColumnsList();

			this.updateCols = opts.getUpdateColumnsOnConflictList();

			if (!opts.getValidateOnly()) {
				if (opts.getTransactionMode() == TransactionMode.PER_STREAM || opts.getTransactionMode() == TransactionMode.PER_BATCH) {
					db.begin();
				}
			}
		}

		void flushCommit(boolean end) {

			if (opts.getValidateOnly()) {
				if (end)
					db.rollback();
				return;
			}
			switch (opts.getTransactionMode()) {
			case PER_ROW -> db.commit();
			case PER_REQUEST -> db.commit();
			case PER_BATCH -> {
				db.commit();
				if (!end)
					db.begin();
			}
			case PER_STREAM -> {
				if (end)
					db.commit();
			}
			default -> {
			}
			}
		}

		InsertSummary summary(Counts c, long startedAtMs) {
			long now = System.currentTimeMillis();
			return InsertSummary.newBuilder().setReceived(c.received).setInserted(c.inserted).setUpdated(c.updated).setIgnored(c.ignored)
					.setFailed(c.failed).addAllErrors(c.errors).setStartedAt(ts(startedAtMs)).setFinishedAt(ts(now)).build();
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

	// --- TX helpers --------------------------------------------------------------

	private static final class TxScope {
		final boolean beganHere;
		final String txId;

		TxScope(boolean beganHere, String txId) {
			this.beganHere = beganHere;
			this.txId = txId;
		}
	}

	/**
	 * Begin a transaction if requested by TransactionContext. Policy: - If tx.begin
	 * == true: begin() and mark beganHere=true (we’re not doing per-tx registry in
	 * this patch). - If tx.timeout_ms > 0 or tx.read_only set: optionally apply
	 * hints (no-ops here unless your DB supports). Returns a TxScope to be passed
	 * to endTx().
	 */
	private TxScope beginTx(Database db, com.arcadedb.server.grpc.TransactionContext tx) {
		if (tx == null)
			return new TxScope(false, null);
		boolean begin = tx.getBegin();
		String txId = tx.getTransactionId().isEmpty() ? null : tx.getTransactionId();

		if (begin) {
			db.begin(); // You can add isolation/RO if your ArcadeDB build supports it
			return new TxScope(true, txId);
		}
		return new TxScope(false, txId);
	}

	/**
	 * End the transaction according to commit/rollback flags. Precedence: rollback
	 * > commit > (if we beganHere and neither flag set) commit()
	 */
	private void endTx(Database db, com.arcadedb.server.grpc.TransactionContext tx, TxScope scope) {
		if (tx == null)
			return;
		try {
			if (tx.getRollback()) {
				db.rollback();
				return;
			}
			if (tx.getCommit()) {
				db.commit();
				return;
			}
			if (scope.beganHere) {
				// Policy for “begin only”: commit by default (you can change to leave-open if
				// you add a tx registry)
				db.commit();
			}
		}
		catch (Exception ignore) {
			// swallow – upstream handler will report the original RPC result
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
					}
					catch (Exception e) {
						// If READ_ONLY fails, try READ_WRITE
						logger.debug("Opening database in READ_WRITE mode: {}", databaseName);
						database = dbFactory.open(ComponentFile.MODE.READ_WRITE);
					}
				}
				else {
					// Create if it doesn't exist
					database = dbFactory.create();
				}

				// Add to pool
				databasePool.put(poolKey, database);
				return database;

			}
			catch (Exception e) {
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
			return value.getListValue().getValuesList().stream().map(this::convertFromProtobufValue).toArray();
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

		Record.Builder builder = Record.newBuilder().setRid(dbRecord.getIdentity().toString());

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
		}
		else if (dbRecord instanceof Vertex) {
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
		}
		else if (dbRecord instanceof Edge) {
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
		}
		else if (value instanceof Boolean) {
			return Value.newBuilder().setBoolValue((Boolean) value).build();
		}
		else if (value instanceof Number) {
			return Value.newBuilder().setNumberValue(((Number) value).doubleValue()).build();
		}
		else if (value instanceof String) {
			return Value.newBuilder().setStringValue((String) value).build();
		}
		else {
			// For complex objects, convert to string
			return Value.newBuilder().setStringValue(value.toString()).build();
		}
	}

	// Proto Value builder from Java (mirror of toJava)
	private com.google.protobuf.Value toProtoValue(Object o) {
		com.google.protobuf.Value.Builder b = com.google.protobuf.Value.newBuilder();
		if (o == null)
			return b.setNullValue(com.google.protobuf.NullValue.NULL_VALUE).build();
		if (o instanceof String s)
			return b.setStringValue(s).build();
		if (o instanceof Number n)
			return b.setNumberValue(n.doubleValue()).build();
		if (o instanceof Boolean bo)
			return b.setBoolValue(bo).build();
		if (o instanceof java.util.Map<?, ?> m) {
			var sb = com.google.protobuf.Struct.newBuilder();
			m.forEach((k, v) -> sb.putFields(String.valueOf(k), toProtoValue(v)));
			return com.google.protobuf.Value.newBuilder().setStructValue(sb).build();
		}
		if (o instanceof java.util.List<?> list) {
			var lb = com.google.protobuf.ListValue.newBuilder();
			list.forEach(v -> lb.addValues(toProtoValue(v)));
			return com.google.protobuf.Value.newBuilder().setListValue(lb).build();
		}
		return b.setStringValue(String.valueOf(o)).build();
	}

	// Apply properties from proto Record to a document/vertex/edge, schema-aware
	// for EMBEDDED
	private void applyGrpcRecord(MutableDocument d, com.arcadedb.server.grpc.Record r, Database db, String targetClass) {
		DocumentType dt = db.getSchema().getType(targetClass);
		r.getPropertiesMap().forEach((k, val) -> d.set(k, toJavaForProperty(db, dt, k, val)));
	}

	private void applyGrpcRecord(MutableVertex v, com.arcadedb.server.grpc.Record r, Database db) {
		DocumentType dt = db.getSchema().getType(v.getTypeName());
		r.getPropertiesMap().forEach((k, val) -> v.set(k, toJavaForProperty(db, dt, k, val)));
	}

	private void applyGrpcRecord(MutableEdge e, com.arcadedb.server.grpc.Record r, Database db, String edgeClass) {
		DocumentType dt = db.getSchema().getType(edgeClass);
		r.getPropertiesMap().forEach((k, val) -> e.set(k, toJavaForProperty(db, dt, k, val)));
	}

	// If property type is EMBEDDED and client sent a Struct/Map, create embedded
	// document
	private Object toJavaForProperty(Database db, DocumentType dt, String key, com.google.protobuf.Value v) {

		var p = (dt != null) ? dt.getPropertyIfExists(key) : null;

		if (p != null && p.getType() == com.arcadedb.schema.Type.EMBEDDED && v.hasStructValue()) {
			MutableDocument emb = db.newDocument(dt.getName());
			v.getStructValue().getFieldsMap().forEach((k2, vv) -> emb.set(k2, toJava(vv)));
			return emb;
		}
		return toJava(v);
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