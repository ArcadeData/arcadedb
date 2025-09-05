package com.arcadedb.server.grpc;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.MutableEmbeddedDocument;
import com.arcadedb.database.RID;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.Index;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import com.arcadedb.serializer.json.JSONObject;
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

			logger.info("executeCommand(): hasTx = {} tx ={}", hasTx, tx);

			if (hasTx && tx.getBegin()) {
				db.begin();
				beganHere = true;
			}

			long affected = 0L;
			
			final boolean returnRows = req.getReturnRows();
			final int maxRows = req.getMaxRows() > 0 ? req.getMaxRows() : DEFAULT_MAX_COMMAND_ROWS;

			ExecuteCommandResponse.Builder out = ExecuteCommandResponse.newBuilder().setSuccess(true).setMessage("OK");

			// Execute the command
			
			logger.info("executeCommand(): command = {}", req.getCommand());
 
			try (ResultSet rs = db.command(language, req.getCommand(), params)) {

				if (rs != null) {
					
					logger.info("executeCommand(): rs = {}", rs);

					if (returnRows) {
						
						logger.info("executeCommand(): returning rows ...");
						
						int emitted = 0;
						
						while (rs.hasNext()) {

							Result r = rs.next();

							if (r.isElement()) {
								affected++; // count modified/returned records
								if (emitted < maxRows) {
									out.addRecords(convertToGrpcRecord(r.getElement().get(), db));
									emitted++;
								}
							}
							else {

								// Scalar / projection row (e.g., RETURN COUNT)

								if (emitted < maxRows) {

									GrpcRecord.Builder recB = GrpcRecord.newBuilder();

									for (String p : r.getPropertyNames()) {

										recB.putProperties(p, convertPropToGrpcValue(p, r));
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
						
						logger.info("executeCommand(): not returning rows ... rs = {}", rs);

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
			}

			logger.info("executeCommand(): after - hasTx = {} tx ={}", hasTx, tx);
			
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
			
			logger.error("ERROR", e);
			
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
			final java.util.Map<String, GrpcValue> props = req.getRecord().getPropertiesMap();

			// --- Vertex ---
			if (dt instanceof com.arcadedb.schema.VertexType) {
				com.arcadedb.graph.MutableVertex v = db.newVertex(cls);

				// apply properties with proper coercion
				props.forEach((k, val) -> v.set(k, toJavaForProperty(db, v, dt, k, val)));

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

					var pv = props.get("out").getValue();
					outStr = pv.hasStringValue() ? pv.getStringValue() : String.valueOf(toJava(pv));
				}

				if (props.containsKey("in")) {

					var pv = props.get("in").getValue();
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
						e.set(k, toJavaForProperty(db, e, dt, k, val));
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
			props.forEach((k, val) -> d.set(k, toJavaForProperty(db, d, dt, k, val)));
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

			resp.onNext(LookupByRidResponse.newBuilder().setFound(true).setRecord(convertToGrpcRecord(el.getRecord(), db)).build());
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

			final var dbRef = db;

			logger.debug("updateRecord(): el = {}; type = {}", el, el.getRecordType());

			if (el instanceof Vertex) {

				logger.debug("updateRecord(): Processing Vertex ...");

				// Get mutable view for updates (works for docs, vertices, edges)
				MutableVertex mvertex = (MutableVertex) el.asVertex(true).modify();

				var dtype = db.getSchema().getType(mvertex.getTypeName());

				// Apply updates

				final java.util.Map<String, GrpcValue> props = req.hasRecord() ? req.getRecord().getPropertiesMap()
						: req.hasPartial() ? req.getPartial().getPropertiesMap() : java.util.Collections.emptyMap();

				// Exclude ArcadeDB system fields during update

				props.forEach((k, v) -> {
					String key = k.trim().toLowerCase();
					if (key.equals("@rid") || key.equals("@type") || key.equals("@cat")) {
						// Skip internal fields to prevent accidental overwrites
						logger.debug("Skipping internal field during update: {}", k);
						return;
					}
					// Perform the update for user-defined fields
					mvertex.set(k, toJavaForProperty(dbRef, mvertex, dtype, k, v));
				});

				mvertex.save();
			}
			else if (el instanceof Document) {

				logger.debug("updateRecord(): Processing Document ...");

				// Get mutable view for updates (works for docs, vertices, edges)
				MutableDocument mdoc = (MutableDocument) el.asDocument(true).modify();

				var dtype = db.getSchema().getType(mdoc.getTypeName());

				// Apply updates

				final java.util.Map<String, GrpcValue> props = req.hasRecord() ? req.getRecord().getPropertiesMap()
						: req.hasPartial() ? req.getPartial().getPropertiesMap() : java.util.Collections.emptyMap();

				// Exclude ArcadeDB system fields during update

				props.forEach((k, v) -> {
					String key = k.trim().toLowerCase();
					if (key.equals("@rid") || key.equals("@type") || key.equals("@cat")) {
						// Skip internal fields to prevent accidental overwrites
						logger.debug("Skipping internal field during update: {}", k);
						return;
					}
					// Perform the update for user-defined fields
					mdoc.set(k, toJavaForProperty(dbRef, mdoc, dtype, k, v));
				});

				mdoc.save();
			}

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

			logger.error("ERROR in updateRecord", e);

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
				
				logger.info("executeQuery(): has Tx {}", request.getTransaction().getTransactionId());
				
				database = activeTransactions.get(request.getTransaction().getTransactionId());
				if (database == null) {
					throw new IllegalArgumentException("Invalid transaction ID");
				}
			}

			// Execute the query
			long startTime = System.currentTimeMillis();
			
			logger.info("executeQuery(): query = {}", request.getQuery());
			
			ResultSet resultSet = database.query("sql", request.getQuery(), convertParameters(request.getParametersMap()));

			logger.info("executeQuery(): to get resultSet = {}", (System.currentTimeMillis() - startTime));

			// Build response
			QueryResult.Builder resultBuilder = QueryResult.newBuilder();

			// Process results
			int count = 0;

			logger.info("executeQuery(): resultSet.size = {}", resultSet.getExactSizeIfKnown());

			while (resultSet.hasNext()) {

				Result result = resultSet.next();

				logger.info("executeQuery(): result = {}", result);

				if (result.isElement()) {

					logger.info("executeQuery(): isElement");

					com.arcadedb.database.Record dbRecord = result.getElement().get();

					GrpcRecord grpcRecord = convertToGrpcRecord(dbRecord, database);
					resultBuilder.addRecords(grpcRecord);
					count++;

					// Apply limit if specified
					if (request.getLimit() > 0 && count >= request.getLimit()) {
						break;
					}
				}
				else {

					logger.info("executeQuery(): NOT isElement");
					
					// Scalar / projection row (e.g., RETURN COUNT)
					
					GrpcRecord.Builder recB = GrpcRecord.newBuilder();

					for (String p : result.getPropertyNames()) {

						recB.putProperties(p, convertPropToGrpcValue(p, result));
					}

					resultBuilder.addRecords(recB.build());
				}
			}

			logger.info("executeQuery(): count = {}",  count);

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
				batch.addRecords(convertToGrpcRecord(rec, db));
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

		final java.util.List<GrpcRecord> all = new java.util.ArrayList<>();

		try (ResultSet rs = db.query("sql", req.getQuery(), convertParameters(req.getParametersMap()))) {

			while (rs.hasNext()) {
				if (cancelled.get())
					return;
				Result r = rs.next();
				if (!r.isElement())
					continue;
				all.add(convertToGrpcRecord(r.getElement().get(), db));
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
					b.addRecords(convertToGrpcRecord(r.getElement().get(), db));
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
								InsertResponse.newBuilder().setStarted(Started.newBuilder().setSessionId(ctx.sessionId).build()).build());

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
									.setBatchAck(BatchAck.newBuilder().setSessionId(ctx.sessionId).setChunkSeq(c.getChunkSeq())
											.setInserted(perChunk.inserted).setUpdated(perChunk.updated).setIgnored(perChunk.ignored)
											.setFailed(perChunk.failed).addAllErrors(perChunk.errors).build())
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

						sendOrQueue.accept(InsertResponse.newBuilder()
								.setBatchAck(BatchAck.newBuilder().setSessionId(ctx.sessionId).setChunkSeq(c.getChunkSeq())
										.setInserted(perChunk.inserted).setUpdated(perChunk.updated).setIgnored(perChunk.ignored)
										.setFailed(perChunk.failed).addAllErrors(perChunk.errors).build())
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

	private boolean tryUpsertVertex(final InsertContext ctx, final com.arcadedb.graph.MutableVertex incoming) {

		var keys = ctx.keyCols;

		if (keys.isEmpty())
			return false;

		// Read incoming values via the (read-only) document view
		var inDoc = incoming.asDocument();

		String where = String.join(" AND ", keys.stream().map(k -> k + " = ?").toList());
		Object[] params = keys.stream().map(inDoc::get).toArray();

		// Prefer selecting the element so we can get a mutable vertex directly
		final String sql = "SELECT FROM " + ctx.opts.getTargetClass() + " WHERE " + where;

		try (var rs = ctx.db.query("sql", sql, params)) {
			if (!rs.hasNext())
				return false;

			var res = rs.next();
			if (!res.isElement())
				return false;

			Vertex v = res.getElement().get().asVertex();

			MutableVertex existingV = v.modify();

			// Apply the updates

			for (String col : ctx.updateCols) {
				existingV.set(col, inDoc.get(col));
			}

			existingV.save();

			return true;
		}
	}

	private boolean tryUpsertDocument(InsertContext ctx, MutableDocument incoming) {

		var keys = ctx.keyCols;

		if (keys.isEmpty())
			return false;

		String where = String.join(" AND ", keys.stream().map(k -> k + " = ?").toList());
		Object[] params = keys.stream().map(incoming::get).toArray();

		try (ResultSet rs = ctx.db.query("sql", "SELECT FROM " + ctx.opts.getTargetClass() + " WHERE " + where, params)) {

			if (!rs.hasNext()) {
				return false;
			}

			var res = rs.next();
			if (!res.isElement())
				return false;

			Document d = res.getElement().get().asDocument();

			var existing = d.modify();

			// Apply the updates

			for (String col : ctx.updateCols) {
				existing.set(col, incoming.get(col));
			}

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

	private Counts insertRows(InsertContext ctx, java.util.Iterator<GrpcRecord> it) {
		Counts c = new Counts();
		int inBatch = 0;

		Schema schema = ctx.db.getSchema();
		DocumentType dt = schema.getType(ctx.opts.getTargetClass());
		boolean isVertex = dt instanceof VertexType;
		boolean isEdge = dt instanceof EdgeType;

		while (it.hasNext()) {

			GrpcRecord r = it.next();

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

	private String getStringProp(GrpcRecord r, String key) {

		if (r == null || key == null || key.isEmpty()) {
			return null;
		}

		if (r.getPropertiesMap().containsKey(key)) {

			GrpcValue grpcVal = r.getPropertiesMap().get(key);

			if (grpcVal.getValueType() == GrpcValueType.STRING || grpcVal.getValue().hasStringValue()) {

				return grpcVal.getValue().getStringValue();
			}

			// Try to convert to string
			return String.valueOf(fromGrpcValue(grpcVal));
		}

		return null;
	}

	private int serverBatchSize(InsertContext ctx) {
		return ctx.opts.getServerBatchSize() == 0 ? 1000 : ctx.opts.getServerBatchSize();
	}

	private void applyGrpcRecord(MutableDocument doc, GrpcRecord r) {
		r.getPropertiesMap().forEach((k, grpcVal) -> {
			// Skip system fields
			if (k.startsWith("@"))
				return;
			doc.set(k, fromGrpcValue(grpcVal));
		});
	}

	private void applyGrpcRecord(MutableVertex vertex, GrpcRecord r) {
		r.getPropertiesMap().forEach((k, grpcVal) -> {
			// Skip system fields
			if (k.startsWith("@"))
				return;
			vertex.set(k, fromGrpcValue(grpcVal));
		});
	}

	private void applyGrpcRecord(MutableEdge edge, GrpcRecord r) {
		r.getPropertiesMap().forEach((k, grpcVal) -> {
			// Skip system fields
			if (k.startsWith("@"))
				return;
			edge.set(k, fromGrpcValue(grpcVal));
		});
	}

	private Object fromGrpcValue(GrpcValue grpcValue) {

		if (grpcValue == null)
			return null;

		Value value = grpcValue.getValue();
		GrpcValueType type = grpcValue.getValueType();

		// Use the type hint to properly convert
		switch (type) {

		case BOOLEAN:
			return value.getBoolValue();

		case BYTE:
			if (value.hasNumberValue())
				return (byte) value.getNumberValue();
			if (value.hasStringValue())
				return Byte.parseByte(value.getStringValue());
			return null;

		case SHORT:
			if (value.hasNumberValue())
				return (short) value.getNumberValue();
			if (value.hasStringValue())
				return Short.parseShort(value.getStringValue());
			return null;

		case INTEGER:
			if (value.hasNumberValue())
				return (int) value.getNumberValue();
			if (value.hasStringValue())
				return Integer.parseInt(value.getStringValue());
			return null;

		case LONG:
			if (value.hasNumberValue())
				return (long) value.getNumberValue();
			if (value.hasStringValue())
				return Long.parseLong(value.getStringValue());
			return null;

		case FLOAT:
			if (value.hasNumberValue())
				return (float) value.getNumberValue();
			if (value.hasStringValue())
				return Float.parseFloat(value.getStringValue());
			return null;

		case DOUBLE:
			return value.getNumberValue();

		case STRING:
			return value.getStringValue();

		case DECIMAL:
			if (value.hasStringValue())
				return new java.math.BigDecimal(value.getStringValue());
			if (value.hasNumberValue())
				return java.math.BigDecimal.valueOf(value.getNumberValue());
			return null;

		case DATE:
			if (value.hasStringValue()) {
				return java.sql.Date.valueOf(java.time.LocalDate.parse(value.getStringValue()));
			}
			if (value.hasNumberValue()) {
				return new java.util.Date((long) value.getNumberValue());
			}
			return null;

		case DATETIME:
			if (value.hasStringValue()) {
				return java.util.Date.from(java.time.Instant.parse(value.getStringValue()));
			}
			if (value.hasNumberValue()) {
				return new java.util.Date((long) value.getNumberValue());
			}
			return null;

		case BINARY:
			if (value.hasStringValue()) {
				return java.util.Base64.getDecoder().decode(value.getStringValue());
			}
			return null;

		case LINK:
			if (value.hasStringValue()) {
				return new com.arcadedb.database.RID(value.getStringValue());
			}
			return null;

		case EMBEDDED:
			// Handle embedded document - create from struct
			if (value.hasStructValue()) {
				// This would need database context to create proper embedded document
				return convertStructToMap(value.getStructValue());
			}
			return null;

		case MAP:
			if (value.hasStructValue()) {
				return convertStructToMap(value.getStructValue());
			}
			return null;

		case LIST:
			if (value.hasListValue()) {
				return value.getListValue().getValuesList().stream().map(this::convertProtobufValueToJava)
						.collect(java.util.stream.Collectors.toList());
			}
			return null;

		default:
			return toJava(value);
		}
	}

	private Map<String, Object> convertStructToMap(com.google.protobuf.Struct struct) {
		if (struct == null) {
			return new java.util.LinkedHashMap<>();
		}

		Map<String, Object> map = new java.util.LinkedHashMap<>();
		struct.getFieldsMap().forEach((key, value) -> {
			map.put(key, convertProtobufValueToJava(value));
		});

		return map;
	}

	private Object convertProtobufValueToJava(com.google.protobuf.Value value) {
		if (value == null) {
			return null;
		}

		switch (value.getKindCase()) {
		case NULL_VALUE:
			return null;

		case NUMBER_VALUE:
			double num = value.getNumberValue();
			// Try to preserve integer types when possible
			if (num == Math.floor(num) && !Double.isInfinite(num)) {
				long longVal = (long) num;
				if (longVal >= Integer.MIN_VALUE && longVal <= Integer.MAX_VALUE) {
					return (int) longVal;
				}
				return longVal;
			}
			return num;

		case STRING_VALUE:
			return value.getStringValue();

		case BOOL_VALUE:
			return value.getBoolValue();

		case STRUCT_VALUE:
			// Recursively convert nested struct
			return convertStructToMap(value.getStructValue());

		case LIST_VALUE:
			// Convert list values
			java.util.List<Object> list = new java.util.ArrayList<>();
			value.getListValue().getValuesList().forEach(item -> {
				list.add(convertProtobufValueToJava(item));
			});
			return list;

		case KIND_NOT_SET:
		default:
			return null;
		}
	}

	// Alternative version that preserves more type information if needed
	private Map<String, Object> convertStructToMapWithTypeHints(com.google.protobuf.Struct struct, Map<String, GrpcValueType> typeHints) {

		if (struct == null) {
			return new java.util.LinkedHashMap<>();
		}

		Map<String, Object> map = new java.util.LinkedHashMap<>();
		struct.getFieldsMap().forEach((key, value) -> {
			GrpcValueType hint = typeHints != null ? typeHints.get(key) : null;
			map.put(key, convertProtobufValueWithTypeHint(value, hint));
		});

		return map;
	}

	private Object convertProtobufValueWithTypeHint(com.google.protobuf.Value value, GrpcValueType typeHint) {

		if (value == null || value.hasNullValue()) {
			return null;
		}

		// If we have a type hint, use it for more precise conversion
		if (typeHint != null) {
			switch (typeHint) {
			case BYTE:
				if (value.hasNumberValue())
					return (byte) value.getNumberValue();
				if (value.hasStringValue())
					return Byte.parseByte(value.getStringValue());
				break;

			case SHORT:
				if (value.hasNumberValue())
					return (short) value.getNumberValue();
				if (value.hasStringValue())
					return Short.parseShort(value.getStringValue());
				break;

			case INTEGER:
				if (value.hasNumberValue())
					return (int) value.getNumberValue();
				if (value.hasStringValue())
					return Integer.parseInt(value.getStringValue());
				break;

			case LONG:
				if (value.hasNumberValue())
					return (long) value.getNumberValue();
				if (value.hasStringValue())
					return Long.parseLong(value.getStringValue());
				break;

			case FLOAT:
				if (value.hasNumberValue())
					return (float) value.getNumberValue();
				if (value.hasStringValue())
					return Float.parseFloat(value.getStringValue());
				break;

			case DECIMAL:
				if (value.hasStringValue())
					return new java.math.BigDecimal(value.getStringValue());
				if (value.hasNumberValue())
					return java.math.BigDecimal.valueOf(value.getNumberValue());
				break;

			case DATE:
				if (value.hasStringValue()) {
					try {
						return java.sql.Date.valueOf(java.time.LocalDate.parse(value.getStringValue()));
					}
					catch (Exception e) {
						logger.debug("Failed to parse date: {}", value.getStringValue());
					}
				}
				if (value.hasNumberValue()) {
					return new java.sql.Date((long) value.getNumberValue());
				}
				break;

			case DATETIME:
				if (value.hasStringValue()) {
					try {
						return java.util.Date.from(java.time.Instant.parse(value.getStringValue()));
					}
					catch (Exception e) {
						logger.debug("Failed to parse datetime: {}", value.getStringValue());
					}
				}
				if (value.hasNumberValue()) {
					return new java.util.Date((long) value.getNumberValue());
				}
				break;

			case BINARY:
				if (value.hasStringValue()) {
					return java.util.Base64.getDecoder().decode(value.getStringValue());
				}
				break;

			case LINK:
				if (value.hasStringValue()) {
					return new com.arcadedb.database.RID(value.getStringValue());
				}
				break;

			default:
				// Fall through to generic conversion
				break;
			}
		}

		// Generic conversion without type hint
		return convertProtobufValueToJava(value);
	}

	private void applyGrpcRecordToDocument(GrpcRecord r, MutableDocument doc) {
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

	private Map<String, Object> convertParameters(Map<String, GrpcValue> protoParams) {

		Map<String, Object> params = new HashMap<>();

		for (Map.Entry<String, GrpcValue> entry : protoParams.entrySet()) {

			params.put(entry.getKey(), fromGrpcValue(entry.getValue()));
		}

		return params;
	}

	private GrpcRecord convertToGrpcRecord(com.arcadedb.database.Record dbRecord, Database db) {

		GrpcRecord.Builder builder = GrpcRecord.newBuilder().setRid(dbRecord.getIdentity().toString());

		logger.info("convertToGrpcRecord(): dbRecord = {}, db = {}", dbRecord, db);
		
		if (dbRecord instanceof Document) {

			Document doc = (Document) dbRecord;
			builder.setType(doc.getTypeName());

			// Get schema information for type-aware conversion
			DocumentType docType = null;
			try {
				docType = db.getSchema().getType(doc.getTypeName());
			}
			catch (Exception e) {
				// Type might not exist in schema
			}

			Set<String> propertyNames = doc.getPropertyNames();
			for (String propertyName : propertyNames) {
				Object value = doc.get(propertyName);
				if (value != null) {
					builder.putProperties(propertyName, convertToGrpcValue(propertyName, value, doc, docType));
				}
			}

			// Add special handling for edges to include endpoints
			if (dbRecord instanceof Edge) {
				Edge edge = (Edge) dbRecord;
				// Add out/in as properties if not already present
				if (!propertyNames.contains("@out")) {
					builder.putProperties("@out", convertToGrpcValue("@out", edge.getOut(), doc, docType));
				}
				if (!propertyNames.contains("@in")) {
					builder.putProperties("@in", convertToGrpcValue("@in", edge.getIn(), doc, docType));
				}
			}
		}
		else {
			
			logger.info("convertToGrpcRecord(): dbRecord = {} not a Document", dbRecord);
		}
		

		return builder.build();
	}
	
	private GrpcValue convertPropToGrpcValue(String propName, Result result) {

		Object propValue = result.getProperty(propName);

		GrpcValue.Builder builder = GrpcValue.newBuilder();

		GrpcValueType valueType = determineValueType(propValue);
		String ofType = inferOfType(propValue);

		builder.setValueType(valueType);
		builder.setValue(toProtoValue(propValue));

		if (ofType != null) {
			builder.setOfType(ofType);
		}

		return builder.build();
	}

	private GrpcValue convertToGrpcValue(String propName, Object value, Document parentDoc, DocumentType docType) {

		GrpcValue.Builder builder = GrpcValue.newBuilder();

		// Try to get schema information for this property
		com.arcadedb.schema.Property schemaProp = null;

		if (docType != null) {
			try {
				schemaProp = docType.getProperty(propName);
			}
			catch (Exception e) {
				// Property not defined in schema
			}
		}

		// Determine the value type
		GrpcValueType valueType;
		String ofType = null;

		if (schemaProp != null) {
			// Use schema type if available
			valueType = schemaTypeToGrpcType(schemaProp.getType());
			if (schemaProp.getOfType() != null) {
				ofType = schemaProp.getOfType();
			}
		}
		else {
			// Infer type from value
			valueType = determineValueType(value);
			ofType = inferOfType(value);
		}

		builder.setValueType(valueType);
		builder.setValue(toProtoValue(value));

		if (ofType != null) {
			builder.setOfType(ofType);
		}

		return builder.build();
	}

	private GrpcValueType schemaTypeToGrpcType(com.arcadedb.schema.Type schemaType) {

		switch (schemaType) {
		case BOOLEAN:
			return GrpcValueType.BOOLEAN;
		case BYTE:
			return GrpcValueType.BYTE;
		case SHORT:
			return GrpcValueType.SHORT;
		case INTEGER:
			return GrpcValueType.INTEGER;
		case LONG:
			return GrpcValueType.LONG;
		case FLOAT:
			return GrpcValueType.FLOAT;
		case DOUBLE:
			return GrpcValueType.DOUBLE;
		case DECIMAL:
			return GrpcValueType.DECIMAL;
		case STRING:
			return GrpcValueType.STRING;
		case BINARY:
			return GrpcValueType.BINARY;
		case DATE:
			return GrpcValueType.DATE;
		case DATETIME:
		case DATETIME_MICROS:
		case DATETIME_NANOS:
		case DATETIME_SECOND:
			return GrpcValueType.DATETIME;
		case LINK:
			return GrpcValueType.LINK;
		case EMBEDDED:
			return GrpcValueType.EMBEDDED;
		case LIST:
			return GrpcValueType.LIST;
		case MAP:
			return GrpcValueType.MAP;
		default:
			return GrpcValueType.STRING;
		}
	}

	private String inferOfType(Object value) {
		if (value instanceof Document) {
			return ((Document) value).getTypeName();
		}
		if (value instanceof Identifiable) {
			try {
				Document linked = ((Identifiable) value).getRecord().asDocument();
				return linked.getTypeName();
			}
			catch (Exception e) {
				return null;
			}
		}
		if (value instanceof List && !((List<?>) value).isEmpty()) {
			Object first = ((List<?>) value).get(0);
			if (first instanceof Document) {
				return ((Document) first).getTypeName();
			}
		}
		return null;
	}

	private GrpcValueType determineValueType(Object value) {
		if (value == null)
			return GrpcValueType.RECORD_VALUE_TYPE_UNSPECIFIED;
		if (value instanceof Boolean)
			return GrpcValueType.BOOLEAN;
		if (value instanceof Byte)
			return GrpcValueType.BYTE;
		if (value instanceof Short)
			return GrpcValueType.SHORT;
		if (value instanceof Integer)
			return GrpcValueType.INTEGER;
		if (value instanceof Long)
			return GrpcValueType.LONG;
		if (value instanceof Float)
			return GrpcValueType.FLOAT;
		if (value instanceof Double)
			return GrpcValueType.DOUBLE;
		if (value instanceof String)
			return GrpcValueType.STRING;
		if (value instanceof java.math.BigDecimal)
			return GrpcValueType.DECIMAL;
		if (value instanceof byte[])
			return GrpcValueType.BINARY;
		if (value instanceof java.util.Date || value instanceof java.time.LocalDate)
			return GrpcValueType.DATE;
		if (value instanceof java.time.Instant || value instanceof java.time.LocalDateTime)
			return GrpcValueType.DATETIME;
		if (value instanceof com.arcadedb.database.RID || value instanceof com.arcadedb.database.Identifiable)
			return GrpcValueType.LINK;
		if (value instanceof com.arcadedb.database.Document)
			return GrpcValueType.EMBEDDED;
		if (value instanceof java.util.Map)
			return GrpcValueType.MAP;
		if (value instanceof java.util.Collection || value.getClass().isArray())
			return GrpcValueType.LIST;

		return GrpcValueType.STRING; // fallback
	}

	private String determineOfType(Object value, String propName, String parentTypeName) {
		if (value instanceof com.arcadedb.database.Document) {
			return ((Document) value).getTypeName();
		}
		if (value instanceof com.arcadedb.database.Identifiable) {
			// For links, return the target type if known
			try {
				Document linked = ((Identifiable) value).getRecord().asDocument();
				return linked.getTypeName();
			}
			catch (Exception e) {
				return null;
			}
		}
		// For collections, you might need schema info to determine element type
		return null;
	}

	private com.google.protobuf.Value toProtoValue(Object o) {
		if (o == null)
			return com.google.protobuf.Value.newBuilder().setNullValue(com.google.protobuf.NullValue.NULL_VALUE).build();

		if (o instanceof String s)
			return com.google.protobuf.Value.newBuilder().setStringValue(s).build();
		if (o instanceof Boolean b)
			return com.google.protobuf.Value.newBuilder().setBoolValue(b).build();
		if (o instanceof Number n)
			return com.google.protobuf.Value.newBuilder().setNumberValue(n.doubleValue()).build();

		if (o instanceof java.math.BigDecimal bd)
			return com.google.protobuf.Value.newBuilder().setStringValue(bd.toPlainString()).build();

		if (o instanceof byte[] bytes)
			return com.google.protobuf.Value.newBuilder().setStringValue(java.util.Base64.getEncoder().encodeToString(bytes)).build();

		// Date/Time → ISO-8601
		if (o instanceof java.util.Date d)
			return com.google.protobuf.Value.newBuilder().setStringValue(d.toInstant().toString()).build();
		if (o instanceof java.time.Instant inst)
			return com.google.protobuf.Value.newBuilder().setStringValue(inst.toString()).build();
		if (o instanceof java.time.LocalDate ld)
			return com.google.protobuf.Value.newBuilder().setStringValue(ld.toString()).build();
		if (o instanceof java.time.LocalDateTime ldt)
			return com.google.protobuf.Value.newBuilder().setStringValue(ldt.toString()).build();

		// Embedded docs (mutable/immutable)
		if (o instanceof com.arcadedb.database.Document doc) {
			var sb = com.google.protobuf.Struct.newBuilder();
			for (String k : doc.getPropertyNames()) {
				sb.putFields(k, toProtoValue(doc.get(k)));
			}
			return com.google.protobuf.Value.newBuilder().setStructValue(sb).build();
		}

		if (o instanceof java.util.Map<?, ?> m) {
			var sb = com.google.protobuf.Struct.newBuilder();
			m.forEach((k, v) -> sb.putFields(String.valueOf(k), toProtoValue(v)));
			return com.google.protobuf.Value.newBuilder().setStructValue(sb).build();
		}

		if (o instanceof java.util.Collection<?> c) {
			var lb = com.google.protobuf.ListValue.newBuilder();
			c.forEach(v -> lb.addValues(toProtoValue(v)));
			return com.google.protobuf.Value.newBuilder().setListValue(lb).build();
		}

		if (o.getClass().isArray()) {
			int len = java.lang.reflect.Array.getLength(o);
			var lb = com.google.protobuf.ListValue.newBuilder();
			for (int i = 0; i < len; i++) {
				lb.addValues(toProtoValue(java.lang.reflect.Array.get(o, i)));
			}
			return com.google.protobuf.Value.newBuilder().setListValue(lb).build();
		}

		// RID or Identifiable → string rid
		if (o instanceof com.arcadedb.database.Identifiable id)
			return com.google.protobuf.Value.newBuilder().setStringValue(id.getIdentity().toString()).build();
		if (o instanceof com.arcadedb.database.RID rid)
			return com.google.protobuf.Value.newBuilder().setStringValue(rid.toString()).build();

		// Fallback
		return com.google.protobuf.Value.newBuilder().setStringValue(String.valueOf(o)).build();
	}

	// Apply properties from proto Record to a document/vertex/edge, schema-aware
	// for EMBEDDED
	private void applyGrpcRecord(MutableDocument d, GrpcRecord r, Database db, String targetClass) {
		DocumentType dt = db.getSchema().getType(targetClass);
		r.getPropertiesMap().forEach((k, val) -> d.set(k, toJavaForProperty(db, d, dt, k, val)));
	}

	private void applyGrpcRecord(MutableVertex v, GrpcRecord r, Database db) {
		DocumentType dt = db.getSchema().getType(v.getTypeName());
		r.getPropertiesMap().forEach((k, val) -> v.set(k, toJavaForProperty(db, v, dt, k, val)));
	}

	private void applyGrpcRecord(MutableEdge e, GrpcRecord r, Database db, String edgeClass) {
		DocumentType dt = db.getSchema().getType(edgeClass);
		r.getPropertiesMap().forEach((k, val) -> e.set(k, toJavaForProperty(db, e, dt, k, val)));
	}

	// Generic fallback (schema-agnostic) for nested values
	private Object toJavaLoose(com.google.protobuf.Value v) {
		return switch (v.getKindCase()) {
		case NULL_VALUE -> null;
		case STRING_VALUE -> v.getStringValue();
		case NUMBER_VALUE -> v.getNumberValue();
		case BOOL_VALUE -> v.getBoolValue();
		case LIST_VALUE -> v.getListValue().getValuesList().stream().map(this::toJavaLoose).toList();
		case STRUCT_VALUE -> {
			var m = new java.util.LinkedHashMap<String, Object>();
			v.getStructValue().getFieldsMap().forEach((k, val) -> m.put(k, toJavaLoose(val)));
			yield m;
		}
		case KIND_NOT_SET -> null;
		};
	}

	private Object toJavaForProperty(final Database db, final MutableDocument parent, final DocumentType dtype, final String propName,
			final GrpcValue grpcValue) {

		if (grpcValue == null)
			return null;

		// Try to get schema information
		com.arcadedb.schema.Property prop = null;
		try {
			prop = (dtype != null) ? dtype.getProperty(propName) : null;
		}
		catch (com.arcadedb.exception.SchemaException e) {
			// Property not in schema, use the type hint from GrpcValue
		}

		// If we have schema info, use it; otherwise use the type from GrpcValue
		if (prop != null) {
			return convertWithSchemaType(db, parent, prop, propName, grpcValue);
		}
		else {
			// No schema, use the type hint from the GrpcValue
			return fromGrpcValue(grpcValue);
		}
	}

	private Object convertWithSchemaType(Database db, MutableDocument parent, com.arcadedb.schema.Property prop, String propName,
			GrpcValue grpcValue) {
		Value v = grpcValue.getValue();
		com.arcadedb.schema.Type schemaType = prop.getType();

		// Handle null values
		if (v.hasNullValue()) {
			return null;
		}

		switch (schemaType) {
		case BOOLEAN:
			if (v.hasBoolValue())
				return v.getBoolValue();
			if (v.hasStringValue())
				return Boolean.parseBoolean(v.getStringValue());
			return null;

		case BYTE:
			if (v.hasNumberValue())
				return (byte) v.getNumberValue();
			if (v.hasStringValue())
				return Byte.parseByte(v.getStringValue());
			return null;

		case SHORT:
			if (v.hasNumberValue())
				return (short) v.getNumberValue();
			if (v.hasStringValue())
				return Short.parseShort(v.getStringValue());
			return null;

		case INTEGER:
			if (v.hasNumberValue())
				return (int) v.getNumberValue();
			if (v.hasStringValue())
				return Integer.parseInt(v.getStringValue());
			return null;

		case LONG:
			if (v.hasNumberValue())
				return (long) v.getNumberValue();
			if (v.hasStringValue())
				return Long.parseLong(v.getStringValue());
			return null;

		case FLOAT:
			if (v.hasNumberValue())
				return (float) v.getNumberValue();
			if (v.hasStringValue())
				return Float.parseFloat(v.getStringValue());
			return null;

		case DOUBLE:
			if (v.hasNumberValue())
				return v.getNumberValue();
			if (v.hasStringValue())
				return Double.parseDouble(v.getStringValue());
			return null;

		case DECIMAL:
			if (v.hasStringValue())
				return new java.math.BigDecimal(v.getStringValue());
			if (v.hasNumberValue())
				return java.math.BigDecimal.valueOf(v.getNumberValue());
			return null;

		case STRING:
			if (v.hasStringValue())
				return v.getStringValue();
			// Convert other types to string representation
			if (v.hasNumberValue())
				return String.valueOf(v.getNumberValue());
			if (v.hasBoolValue())
				return String.valueOf(v.getBoolValue());
			return null;

		case BINARY:
			if (v.hasStringValue()) {
				// Expect base64 encoded string
				return java.util.Base64.getDecoder().decode(v.getStringValue());
			}
			return null;

		case DATE:
			if (v.hasStringValue()) {
				// Parse ISO date format (yyyy-MM-dd)
				return java.sql.Date.valueOf(java.time.LocalDate.parse(v.getStringValue()));
			}
			if (v.hasNumberValue()) {
				// Epoch millis
				return new java.sql.Date((long) v.getNumberValue());
			}
			return null;

		case DATETIME:
		case DATETIME_MICROS:
		case DATETIME_NANOS:
		case DATETIME_SECOND:
			if (v.hasStringValue()) {
				// Parse ISO-8601 datetime
				java.time.Instant instant = java.time.Instant.parse(v.getStringValue());
				return java.util.Date.from(instant);
			}
			if (v.hasNumberValue()) {
				// Epoch millis
				return new java.util.Date((long) v.getNumberValue());
			}
			return null;

		case LINK:
			if (v.hasStringValue()) {
				// Parse RID string format (#cluster:position)
				return new com.arcadedb.database.RID(v.getStringValue());
			}
			return null;

		case EMBEDDED:
			if (v.hasStructValue()) {
				String embeddedTypeName = grpcValue.hasOfType() ? grpcValue.getOfType() : prop.getOfType();

				MutableEmbeddedDocument ed = parent.newEmbeddedDocument(embeddedTypeName, propName);

				// Recursively convert struct fields
				v.getStructValue().getFieldsMap().forEach((k, vv) -> {
					// Try to get schema for embedded type
					DocumentType embeddedType = null;
					com.arcadedb.schema.Property embeddedProp = null;

					if (embeddedTypeName != null) {
						try {
							embeddedType = db.getSchema().getType(embeddedTypeName);
							embeddedProp = embeddedType.getProperty(k);
						}
						catch (Exception e) {
							// Property not in schema
						}
					}

					if (embeddedProp != null && embeddedType != null) {
						// Create a simple GrpcValue wrapper for recursive conversion
						GrpcValue.Builder rvBuilder = GrpcValue.newBuilder().setValue(vv)
								.setValueType(schemaTypeToGrpcType(embeddedProp.getType()));
						ed.set(k, convertWithSchemaType(db, ed, embeddedProp, k, rvBuilder.build()));
					}
					else {
						// No schema, use generic conversion
						ed.set(k, toJava(vv));
					}
				});
				return ed;
			}
			return null;

		case LIST:
		case ARRAY_OF_SHORTS:
		case ARRAY_OF_INTEGERS:
		case ARRAY_OF_LONGS:
		case ARRAY_OF_FLOATS:
		case ARRAY_OF_DOUBLES:
			if (v.hasListValue()) {
				java.util.List<Object> list = new java.util.ArrayList<>();

				// Determine element type
				com.arcadedb.schema.Type elementType = getListElementType(schemaType);

				for (com.google.protobuf.Value item : v.getListValue().getValuesList()) {
					if (elementType != null) {
						// Create wrapper for typed conversion
						GrpcValue.Builder rvBuilder = GrpcValue.newBuilder().setValue(item).setValueType(schemaTypeToGrpcType(elementType));

						// Create a dummy property for element conversion
						com.arcadedb.schema.Property elementProp = createVirtualProperty(elementType, prop.getOfType());
						list.add(convertWithSchemaType(db, parent, elementProp, propName + "[element]", rvBuilder.build()));
					}
					else {
						// Generic conversion
						list.add(toJava(item));
					}
				}

				// Convert to appropriate array type if needed
				if (schemaType == com.arcadedb.schema.Type.ARRAY_OF_SHORTS) {
					return list.stream().mapToInt(o -> ((Number) o).shortValue()).toArray();
				}
				else if (schemaType == com.arcadedb.schema.Type.ARRAY_OF_INTEGERS) {
					return list.stream().mapToInt(o -> ((Number) o).intValue()).toArray();
				}
				else if (schemaType == com.arcadedb.schema.Type.ARRAY_OF_LONGS) {
					return list.stream().mapToLong(o -> ((Number) o).longValue()).toArray();
				}
				else if (schemaType == com.arcadedb.schema.Type.ARRAY_OF_FLOATS) {
					float[] array = new float[list.size()];
					for (int i = 0; i < list.size(); i++) {
						array[i] = ((Number) list.get(i)).floatValue();
					}
					return array;
				}
				else if (schemaType == com.arcadedb.schema.Type.ARRAY_OF_DOUBLES) {
					return list.stream().mapToDouble(o -> ((Number) o).doubleValue()).toArray();
				}

				return list;
			}
			return null;

		case MAP:
			if (v.hasStructValue()) {
				java.util.Map<String, Object> map = new java.util.LinkedHashMap<>();
				v.getStructValue().getFieldsMap().forEach((k, vv) -> {
					map.put(k, toJava(vv));
				});
				return map;
			}
			return null;

		default:
			// Fallback to generic conversion
			return toJava(v);
		}
	}

	private com.arcadedb.schema.Property createVirtualProperty(final com.arcadedb.schema.Type type, final String ofType) {

		return new com.arcadedb.schema.Property() {
			private final Map<String, Object> customValues = new HashMap<>();
			private Object defaultValue = null;
			private boolean readonly = false;
			private boolean mandatory = false;
			private boolean notNull = false;
			private boolean hidden = false;
			private String max = null;
			private String min = null;
			private String regexp = null;

			@Override
			public Index createIndex(Schema.INDEX_TYPE indexType, boolean unique) {
				// Virtual properties cannot create indexes
				throw new UnsupportedOperationException("Cannot create index on virtual property");
			}

			@Override
			public Index getOrCreateIndex(Schema.INDEX_TYPE indexType, boolean unique) {
				// Virtual properties don't have indexes
				return null;
			}

			@Override
			public String getName() {
				return "__virtual_" + type.name().toLowerCase();
			}

			@Override
			public Type getType() {
				return type;
			}

			@Override
			public int getId() {
				// Virtual properties don't have database IDs
				return -1;
			}

			@Override
			public Object getDefaultValue() {
				return defaultValue;
			}

			@Override
			public Property setDefaultValue(Object defaultValue) {
				this.defaultValue = defaultValue;
				return this;
			}

			@Override
			public String getOfType() {
				return ofType;
			}

			@Override
			public Property setOfType(String ofType) {
				// Cannot modify virtual property's ofType after creation
				throw new UnsupportedOperationException("Cannot modify virtual property's ofType");
			}

			@Override
			public Property setReadonly(boolean readonly) {
				this.readonly = readonly;
				return this;
			}

			@Override
			public boolean isReadonly() {
				return readonly;
			}

			@Override
			public Property setMandatory(boolean mandatory) {
				this.mandatory = mandatory;
				return this;
			}

			@Override
			public boolean isMandatory() {
				return mandatory;
			}

			@Override
			public Property setNotNull(boolean notNull) {
				this.notNull = notNull;
				return this;
			}

			@Override
			public boolean isNotNull() {
				return notNull;
			}

			@Override
			public Property setHidden(boolean hidden) {
				this.hidden = hidden;
				return this;
			}

			@Override
			public boolean isHidden() {
				return hidden;
			}

			@Override
			public Property setMax(String max) {
				this.max = max;
				return this;
			}

			@Override
			public String getMax() {
				return max;
			}

			@Override
			public Property setMin(String min) {
				this.min = min;
				return this;
			}

			@Override
			public String getMin() {
				return min;
			}

			@Override
			public Property setRegexp(String regexp) {
				this.regexp = regexp;
				return this;
			}

			@Override
			public String getRegexp() {
				return regexp;
			}

			@Override
			public Set<String> getCustomKeys() {
				return customValues.keySet();
			}

			@Override
			public Object getCustomValue(String key) {
				return customValues.get(key);
			}

			@Override
			public Object setCustomValue(String key, Object value) {
				return customValues.put(key, value);
			}

			@Override
			public JSONObject toJSON() {
				JSONObject json = new JSONObject();
				json.put("name", getName());
				json.put("type", type.name());
				if (ofType != null) {
					json.put("ofType", ofType);
				}
				if (defaultValue != null) {
					json.put("default", defaultValue);
				}
				json.put("readonly", readonly);
				json.put("mandatory", mandatory);
				json.put("notNull", notNull);
				json.put("hidden", hidden);
				if (max != null)
					json.put("max", max);
				if (min != null)
					json.put("min", min);
				if (regexp != null)
					json.put("regexp", regexp);
				if (!customValues.isEmpty()) {
					json.put("custom", new JSONObject(customValues));
				}
				return json;
			}

			@Override
			public boolean equals(Object obj) {
				if (this == obj)
					return true;
				if (!(obj instanceof com.arcadedb.schema.Property))
					return false;
				com.arcadedb.schema.Property other = (com.arcadedb.schema.Property) obj;
				return type == other.getType() && Objects.equals(ofType, other.getOfType());
			}

			@Override
			public int hashCode() {
				return Objects.hash(type, ofType);
			}

			@Override
			public String toString() {
				return "VirtualProperty{type=" + type + (ofType != null ? ", ofType=" + ofType : "") + "}";
			}
		};
	}

	// Helper method to determine element type for arrays/lists
	private com.arcadedb.schema.Type getListElementType(com.arcadedb.schema.Type listType) {
		switch (listType) {
		case ARRAY_OF_SHORTS:
			return com.arcadedb.schema.Type.SHORT;
		case ARRAY_OF_INTEGERS:
			return com.arcadedb.schema.Type.INTEGER;
		case ARRAY_OF_LONGS:
			return com.arcadedb.schema.Type.LONG;
		case ARRAY_OF_FLOATS:
			return com.arcadedb.schema.Type.FLOAT;
		case ARRAY_OF_DOUBLES:
			return com.arcadedb.schema.Type.DOUBLE;
		default:
			return null;
		}
	}

	private String generateTransactionId() {
		return "tx_" + System.nanoTime();
	}
}