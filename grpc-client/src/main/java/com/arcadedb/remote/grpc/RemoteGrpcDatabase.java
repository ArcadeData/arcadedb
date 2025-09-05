package com.arcadedb.remote.grpc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.remote.RemoteException;
import com.arcadedb.remote.RemoteImmutableDocument;
import com.arcadedb.remote.RemoteImmutableEdge;
import com.arcadedb.remote.RemoteImmutableVertex;
import com.arcadedb.remote.RemoteSchema;
import com.arcadedb.remote.RemoteTransactionExplicitLock;
import com.arcadedb.remote.grpc.utils.ProtoUtils;
import com.arcadedb.server.grpc.ArcadeDbServiceGrpc;
import com.arcadedb.server.grpc.BatchAck;
import com.arcadedb.server.grpc.BeginTransactionRequest;
import com.arcadedb.server.grpc.BeginTransactionResponse;
import com.arcadedb.server.grpc.BulkInsertRequest;
import com.arcadedb.server.grpc.Commit;
import com.arcadedb.server.grpc.CommitTransactionRequest;
import com.arcadedb.server.grpc.CommitTransactionResponse;
import com.arcadedb.server.grpc.CreateRecordRequest;
import com.arcadedb.server.grpc.CreateRecordResponse;
import com.arcadedb.server.grpc.DatabaseCredentials;
import com.arcadedb.server.grpc.DeleteRecordRequest;
import com.arcadedb.server.grpc.DeleteRecordResponse;
import com.arcadedb.server.grpc.ExecuteCommandRequest;
import com.arcadedb.server.grpc.ExecuteCommandResponse;
import com.arcadedb.server.grpc.ExecuteQueryRequest;
import com.arcadedb.server.grpc.ExecuteQueryResponse;
import com.arcadedb.server.grpc.GrpcRecord;
import com.arcadedb.server.grpc.GrpcValue;
import com.arcadedb.server.grpc.InsertChunk;
import com.arcadedb.server.grpc.InsertOptions;
import com.arcadedb.server.grpc.InsertRequest;
import com.arcadedb.server.grpc.InsertResponse;
import com.arcadedb.server.grpc.InsertSummary;
import com.arcadedb.server.grpc.LookupByRidRequest;
import com.arcadedb.server.grpc.LookupByRidResponse;
import com.arcadedb.server.grpc.PropertiesUpdate;
import com.arcadedb.server.grpc.QueryResult;
import com.arcadedb.server.grpc.RollbackTransactionRequest;
import com.arcadedb.server.grpc.RollbackTransactionResponse;
import com.arcadedb.server.grpc.Start;
import com.arcadedb.server.grpc.StreamQueryRequest;
import com.arcadedb.server.grpc.TransactionContext;
import com.arcadedb.server.grpc.TransactionIsolation;
import com.arcadedb.server.grpc.UpdateRecordRequest;
import com.arcadedb.server.grpc.UpdateRecordResponse;
import com.google.protobuf.Value;

import io.grpc.CallCredentials;
import io.grpc.CompressorRegistry;
import io.grpc.DecompressorRegistry;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.BlockingClientCall;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;

/**
 * Remote Database implementation using gRPC protocol instead of HTTP. Extends
 * RemoteDatabase to maintain compatibility while overriding network operations.
 * It's not thread safe. For multi-thread usage create one instance of
 * RemoteGrpcDatabase per thread.
 *
 * @author Oleg Cohen (oleg.cohen@gmail.com)
 */
public class RemoteGrpcDatabase extends RemoteDatabase {

	private final ManagedChannel channel;

	private final ArcadeDbServiceGrpc.ArcadeDbServiceBlockingV2Stub blockingStub;
	private final ArcadeDbServiceGrpc.ArcadeDbServiceStub asyncStub;

	private String transactionId;

	private final RemoteSchema schema;

	private final String userName;
	private final String userPassword;
	private String databaseName;
	private RemoteTransactionExplicitLock explicitLock;

	public RemoteGrpcDatabase(final String server, final int grpcPort, final int httpPort, final String databaseName, final String userName,
			final String userPassword) {
		this(server, grpcPort, httpPort, databaseName, userName, userPassword, new ContextConfiguration());
	}

	public RemoteGrpcDatabase(final String server, final int grpcPort, final int httpPort, final String databaseName, final String userName,
			final String userPassword, final ContextConfiguration configuration) {

		super(server, httpPort, databaseName, userName, userPassword, configuration);

		this.userName = userName;
		this.userPassword = userPassword;

		this.databaseName = databaseName;

		// Create gRPC channel
		this.channel = createChannel(server, grpcPort);

		// Create stubs with authentication
		CallCredentials credentials = createCallCredentials(userName, userPassword);

		this.blockingStub = createBlockingStub(channel);

		this.asyncStub = createAsyncStub(channel);

		// Create gRPC-specific schema
		this.schema = new RemoteSchema(this);
	}

	/**
	 * Creates the gRPC channel. Override this for custom channel configuration.
	 */
	protected ManagedChannel createChannel(String server, int port) {
		return ManagedChannelBuilder.forAddress(server, port).usePlaintext() // No TLS by default
				.build();
	}

	/**
	 * Creates call credentials for authentication
	 */
	protected CallCredentials createCallCredentials(String userName, String userPassword) {
		return new CallCredentials() {
			@Override
			public void applyRequestMetadata(RequestInfo requestInfo, Executor appExecutor, MetadataApplier applier) {
				Metadata headers = new Metadata();
				headers.put(Metadata.Key.of("username", Metadata.ASCII_STRING_MARSHALLER), userName);
				headers.put(Metadata.Key.of("password", Metadata.ASCII_STRING_MARSHALLER), userPassword);
				headers.put(Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER), userName);
				headers.put(Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER), userPassword);
				headers.put(Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER), databaseName);
				applier.apply(headers);
			}

			@Override
			public void thisUsesUnstableApi() {
				// Required by the interface
			}
		};
	}

	protected CallCredentials createCredentials() {
		return createCallCredentials(userName, userPassword);
	}

	/**
	 * Override this method to customize blocking stub creation
	 */
	protected ArcadeDbServiceGrpc.ArcadeDbServiceBlockingV2Stub createBlockingStub(ManagedChannel channel) {
		return ArcadeDbServiceGrpc.newBlockingV2Stub(channel).withCallCredentials(createCredentials());
	}

	protected ArcadeDbServiceGrpc.ArcadeDbServiceStub createAsyncStub(ManagedChannel channel) {
		return ArcadeDbServiceGrpc.newStub(channel).withCallCredentials(createCredentials()).withCompression("gzip");
	}

	@Override
	public String getDatabasePath() {
		return "grpc://" + channel.authority() + "/" + getName();
	}

	@Override
	public RemoteSchema getSchema() {
		return schema;
	}

	@Override
	public void close() {
		if (transactionId != null) {
			rollback();
		}
		super.close();

		// Shutdown gRPC channel
		channel.shutdown();
		try {
			if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
				channel.shutdownNow();
			}
		} catch (InterruptedException e) {
			channel.shutdownNow();
		}
	}

	@Override
	public void begin(final Database.TRANSACTION_ISOLATION_LEVEL isolationLevel) {
		checkDatabaseIsOpen();
		if (transactionId != null)
			throw new TransactionException("Transaction already begun");

		BeginTransactionRequest request = BeginTransactionRequest.newBuilder().setDatabase(getName()).setCredentials(buildCredentials())
				.setIsolation(mapIsolationLevel(isolationLevel)).build();

		try {
			BeginTransactionResponse response = blockingStub.withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS).beginTransaction(request);
			transactionId = response.getTransactionId();
			// Store transaction ID in parent class session management
			setSessionId(transactionId);
		} catch (StatusRuntimeException e) {
			throw new TransactionException("Error on transaction begin", e);
		} catch (StatusException e) {
			throw new TransactionException("Error on transaction begin", e);
		}
	}

	@Override
	public void commit() {
		checkDatabaseIsOpen();
		stats.txCommits.incrementAndGet();

		if (transactionId == null)
			throw new TransactionException("Transaction not begun");

		CommitTransactionRequest request = CommitTransactionRequest.newBuilder()
				.setTransaction(TransactionContext.newBuilder().setTransactionId(transactionId).setDatabase(getName()).build())
				.setCredentials(buildCredentials()).build();

		try {
			CommitTransactionResponse response = blockingStub.withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS).commitTransaction(request);
			if (!response.getSuccess()) {
				throw new TransactionException("Failed to commit transaction: " + response.getMessage());
			}
		} catch (StatusRuntimeException e) {
			handleGrpcException(e);
		} catch (StatusException e) {
			handleGrpcException(e);
		} finally {
			transactionId = null;
			setSessionId(null);
		}
	}

	@Override
	public void rollback() {
		checkDatabaseIsOpen();
		stats.txRollbacks.incrementAndGet();

		if (transactionId == null)
			throw new TransactionException("Transaction not begun");

		RollbackTransactionRequest request = RollbackTransactionRequest.newBuilder()
				.setTransaction(TransactionContext.newBuilder().setTransactionId(transactionId).setDatabase(getName()).build())
				.setCredentials(buildCredentials()).build();

		try {
			RollbackTransactionResponse response = blockingStub.withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS)
					.rollbackTransaction(request);
			if (!response.getSuccess()) {
				throw new TransactionException("Failed to rollback transaction: " + response.getMessage());
			}
		} catch (StatusRuntimeException e) {
			throw new TransactionException("Error on transaction rollback", e);
		} catch (StatusException e) {
			throw new TransactionException("Error on transaction rollback", e);
		} finally {
			transactionId = null;
			setSessionId(null);
		}
	}

	@Override
	public void deleteRecord(final Record record) {
		checkDatabaseIsOpen();
		stats.deleteRecord.incrementAndGet();

		if (record.getIdentity() == null)
			throw new IllegalArgumentException("Cannot delete a non persistent record");

		DeleteRecordRequest request = DeleteRecordRequest.newBuilder().setDatabase(getName()).setRid(record.getIdentity().toString())
				.setCredentials(buildCredentials()).build();

		try {
			DeleteRecordResponse response = blockingStub.withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS).deleteRecord(request);
			if (!response.getSuccess()) {
				throw new DatabaseOperationException("Failed to delete record: " + response.getMessage());
			}
		} catch (StatusRuntimeException e) {
			handleGrpcException(e);
		} catch (StatusException e) {
			handleGrpcException(e);
		}
	}

	@Override
	public Iterator<Record> iterateType(final String typeName, final boolean polymorphic) {
		String query = "select from `" + typeName + "`";
		if (!polymorphic)
			query += " where @type = '" + typeName + "'";
		return streamQuery(query);
	}

	@Override
	public Iterator<Record> iterateBucket(final String bucketName) {
		return streamQuery("select from bucket:`" + bucketName + "`");
	}

	@Override
	public ResultSet query(final String language, final String query, final Map<String, Object> params) {
		checkDatabaseIsOpen();
		stats.queries.incrementAndGet();

		ExecuteQueryRequest.Builder requestBuilder = ExecuteQueryRequest.newBuilder().setDatabase(getName()).setQuery(query)
				.setCredentials(buildCredentials());

		if (transactionId != null) {
			requestBuilder.setTransaction(TransactionContext.newBuilder().setTransactionId(transactionId).setDatabase(getName()).build());
		}

		if (params != null && !params.isEmpty()) {
			requestBuilder.putAllParameters(convertParamsToGrpcValue(params));
		}

		try {
		
			ExecuteQueryResponse response = blockingStub.withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS)
					.executeQuery(requestBuilder.build());
			
			return createGrpcResultSet(response);
		} 
		catch (StatusRuntimeException e) {
			handleGrpcException(e);
			return new InternalResultSet();
		} 
		catch (StatusException e) {
			handleGrpcException(e);
			return new InternalResultSet();
		}
	}

	@Override
	public ResultSet query(final String language, final String query, final Object... args) {
		checkDatabaseIsOpen();
		stats.queries.incrementAndGet();

		final Map<String, Object> params = mapArgs(args);

		return query(language, query, params);
	}

	@Override
	public ResultSet command(final String language, final String command, final Map<String, Object> params) {
		checkDatabaseIsOpen();
		stats.commands.incrementAndGet();

		ExecuteCommandRequest.Builder requestBuilder = ExecuteCommandRequest.newBuilder().setDatabase(getName()).setCommand(command)
				.setCredentials(buildCredentials());

		if (transactionId != null) {
			requestBuilder.setTransaction(TransactionContext.newBuilder().setTransactionId(transactionId).setDatabase(getName()).build());
		}

		if (params != null && !params.isEmpty()) {
			requestBuilder.putAllParameters(convertParamsToGrpcValue(params));
		}

		try {
			
			ExecuteCommandResponse response = blockingStub.withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS)
					.executeCommand(requestBuilder.build());
			
			if (! response.getSuccess()) {
				throw new DatabaseOperationException("Failed to execute command: " + response.getMessage());
			}

			// Create result set with command execution info
			InternalResultSet resultSet = new InternalResultSet();
			
			if (response.getAffectedRecords() > 0) {
				Map<String, Object> result = new HashMap<>();
				result.put("affected", response.getAffectedRecords());
				result.put("executionTime", response.getExecutionTimeMs());
				resultSet.add(new ResultInternal(result));
			}
			return resultSet;
		} catch (StatusRuntimeException e) {
			handleGrpcException(e);
			return new InternalResultSet();
		} catch (StatusException e) {
			handleGrpcException(e);
			return new InternalResultSet();
		}
	}

	public ExecuteCommandResponse execSql(String db, String sql, Map<String, Object> params, long timeoutMs) {
		return executeCommand(db, "sql", sql, params, /* returnRows */ false, /* maxRows */ 0, txBeginCommit(), timeoutMs);
	}

	public ExecuteCommandResponse execSql(String sql, Map<String, Object> params, long timeoutMs) {
		return executeCommand(databaseName, "sql", sql, params, /* returnRows */ false, /* maxRows */ 0, txBeginCommit(), timeoutMs);
	}

	public ExecuteCommandResponse executeCommand(String language, String command, Map<String, Object> params,
			boolean returnRows, int maxRows, TransactionContext tx, long timeoutMs) {

		var reqB = ExecuteCommandRequest.newBuilder().setDatabase(databaseName).setCommand(command)
				.putAllParameters(convertParamsToGrpcValue(params)).setLanguage(langOrDefault(language)).setReturnRows(returnRows)
				.setMaxRows(maxRows > 0 ? maxRows : 0);

		if (tx != null)
			reqB.setTransaction(tx);
		// credentials: if your stub builds creds implicitly, set here if required
		reqB.setCredentials(buildCredentials());

		try {
			return blockingStub.withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS).executeCommand(reqB.build());
		} catch (StatusException e) {
			throw new RuntimeException("Failed to execute command: " + e.getMessage(), e);
		}
	}

	public ExecuteCommandResponse executeCommand(String database, String language, String command,
			Map<String, Object> params, boolean returnRows, int maxRows, TransactionContext tx, long timeoutMs) {

		var reqB = ExecuteCommandRequest.newBuilder().setDatabase(database).setCommand(command)
				.putAllParameters(convertParamsToGrpcValue(params)).setLanguage(langOrDefault(language)).setReturnRows(returnRows)
				.setMaxRows(maxRows > 0 ? maxRows : 0);

		if (tx != null)
			reqB.setTransaction(tx);
		// credentials: if your stub builds creds implicitly, set here if required
		reqB.setCredentials(buildCredentials());

		try {
			return blockingStub.withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS).executeCommand(reqB.build());
		} catch (StatusException e) {
			throw new RuntimeException("Failed to execute command: " + e.getMessage(), e);
		}
	}

	@Override
	protected RID saveRecord(final MutableDocument record) {
		stats.createRecord.incrementAndGet();

		final RID rid = record.getIdentity();

		if (rid != null) {
			// -------- UPDATE (partial) --------
			PropertiesUpdate partial = PropertiesUpdate.newBuilder()
					.putAllProperties(convertParamsToGrpcValue(record.toMap(false)))
					.build();

			UpdateRecordRequest request = UpdateRecordRequest.newBuilder()
					.setDatabase(getName())
					.setRid(rid.toString())
					.setPartial(partial)
					.setDatabase(databaseName)
					.setCredentials(buildCredentials()).build();

			try {
				UpdateRecordResponse response = blockingStub
						.withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS)
						.updateRecord(request);

				// If your proto has flags, you can check response.getSuccess()/getUpdated()
				// Otherwise, treat non-exception as success.
				return rid;
			} catch (StatusRuntimeException e) {
				handleGrpcException(e);
				return null;
			} catch (StatusException e) {
				handleGrpcException(e);
				return null;
			}
		} else {
			// -------- CREATE --------
			GrpcRecord recMsg = GrpcRecord.newBuilder()
					.putAllProperties(convertParamsToGrpcValue(record.toMap(false)))
					.build();

			CreateRecordRequest request = CreateRecordRequest.newBuilder()
					.setDatabase(getName())
					.setType(record.getTypeName())
					.setRecord(recMsg) // nested GrpcRecord payload
					.setCredentials(buildCredentials()).build();

			try {
				CreateRecordResponse response = blockingStub
						.withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS)
						.createRecord(request);

				// Proto returns the newly created RID as a string
				final String ridStr = response.getRid();
				if (ridStr == null || ridStr.isEmpty()) {
					throw new DatabaseOperationException("Failed to create record (empty RID)");
				}

				// Construct a RID from the returned string
				try {
					return new RID(ridStr);
				} catch (NoSuchMethodError | IllegalArgumentException ex) {
					// Fallback for older APIs expecting (Database, String)
					return new RID(this, ridStr);
				}
			} catch (StatusRuntimeException e) {
				handleGrpcException(e);
				return null;
			} catch (StatusException e) {
				handleGrpcException(e);
				return null;
			}
		}
	}

	// Convenience: default batch size stays 100, default mode = CURSOR
	public Iterator<Record> queryStream(final String language, final String query) {
		return queryStream(language, query, /* batchSize */100, StreamQueryRequest.RetrievalMode.CURSOR);
	}

	public Iterator<Record> queryStream(final String language, final String query, final int batchSize) {
		return queryStream(language, query, batchSize, StreamQueryRequest.RetrievalMode.CURSOR);
	}

	// NEW: choose retrieval mode
	public Iterator<Record> queryStream(final String language, final String query, final int batchSize,
			final StreamQueryRequest.RetrievalMode mode) {
		checkDatabaseIsOpen();
		stats.queries.incrementAndGet();

		StreamQueryRequest request = StreamQueryRequest.newBuilder().setDatabase(getName()).setQuery(query).setCredentials(buildCredentials())
				.setBatchSize(batchSize > 0 ? batchSize : 100).setRetrievalMode(mode) // <--- NEW
				.build();

		final BlockingClientCall<?, QueryResult> responseIterator = blockingStub.withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS)
				.streamQuery(request);

		return new Iterator<Record>() {
			private Iterator<GrpcRecord> currentBatch = Collections.emptyIterator();

			@Override
			public boolean hasNext() {
				if (currentBatch.hasNext())
					return true;

				try {
					while (responseIterator.hasNext()) {
						QueryResult result = responseIterator.read();

						// Defensive: skip any empty batches the server might send
						if (result.getRecordsCount() == 0) {
							if (result.getIsLastBatch())
								return false;
							continue;
						}

						currentBatch = result.getRecordsList().iterator();
						return true;
					}
				}
				catch (InterruptedException e) {
					throw new RuntimeException("Interrupted while waiting for stream", e);
				}
				catch (StatusException e) {
					handleGrpcException(e);
				}

				return false;
			}

			@Override
			public Record next() {
				if (!hasNext())
					throw new NoSuchElementException();
				return grpcRecordToDBRecord(currentBatch.next());
			}
		};
	}

	// PARAMETERIZED variant with retrieval mode
	public Iterator<Record> queryStream(final String language, final String query, final Map<String, Object> params, final int batchSize,
			final StreamQueryRequest.RetrievalMode mode) {
		checkDatabaseIsOpen();
		stats.queries.incrementAndGet();

		StreamQueryRequest.Builder b = StreamQueryRequest.newBuilder().setDatabase(getName()).setQuery(query).setCredentials(buildCredentials())
				.setBatchSize(batchSize > 0 ? batchSize : 100).setRetrievalMode(mode); // <--- NEW

		if (params != null && !params.isEmpty()) {
			b.putAllParameters(convertParamsToGrpcValue(params));
		}

		final BlockingClientCall<?, QueryResult> responseIterator = blockingStub.withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS)
				.streamQuery(b.build());

		return new Iterator<Record>() {
			private Iterator<GrpcRecord> currentBatch = Collections.emptyIterator();

			@Override
			public boolean hasNext() {
				if (currentBatch.hasNext())
					return true;

				try {
					
					while (responseIterator.hasNext()) {
						
						QueryResult result = responseIterator.read();

						if (result.getRecordsCount() == 0) {
							if (result.getIsLastBatch())
								return false;
							continue;
						}

						currentBatch = result.getRecordsList().iterator();
						return true;
					}

					return false;
				}
				catch (Exception e) {
					
					throw new RuntimeException("Interrupted while waiting for stream", e);
				}				
			}

			@Override
			public Record next() {
				if (!hasNext())
					throw new NoSuchElementException();
				return grpcRecordToDBRecord(currentBatch.next());
			}
		};
	}

	// Keep the old signature working (defaults to CURSOR)
	public Iterator<Record> queryStream(final String language, final String query, final Map<String, Object> params, final int batchSize) {
		return queryStream(language, query, params, batchSize, StreamQueryRequest.RetrievalMode.CURSOR);
	}

	public static final class QueryBatch {
		private final List<Record> records;
		private final int totalInBatch;
		private final long runningTotal;
		private final boolean lastBatch;

		public QueryBatch(List<Record> list, int totalInBatch, long runningTotal, boolean lastBatch) {
			this.records = list;
			this.totalInBatch = totalInBatch;
			this.runningTotal = runningTotal;
			this.lastBatch = lastBatch;
		}

		public List<Record> records() {
			return records;
		}

		public int totalInBatch() {
			return totalInBatch;
		}

		public long runningTotal() {
			return runningTotal;
		}

		public boolean isLastBatch() {
			return lastBatch;
		}
	}

	public Iterator<QueryBatch> queryStreamBatches(final String language, final String query, final Map<String, Object> params,
			final int batchSize, final StreamQueryRequest.RetrievalMode mode) {
		checkDatabaseIsOpen();
		stats.queries.incrementAndGet();

		StreamQueryRequest.Builder b = StreamQueryRequest.newBuilder().setDatabase(getName()).setQuery(query).setCredentials(buildCredentials())
				.setBatchSize(batchSize > 0 ? batchSize : 100).setRetrievalMode(mode);

		if (params != null && !params.isEmpty()) {
			b.putAllParameters(convertParamsToGrpcValue(params));
		}

		final BlockingClientCall<?, QueryResult> responseIterator = blockingStub.withWaitForReady() // optional, improves robustness
				.withDeadlineAfter(/* e.g. */ 10, TimeUnit.MINUTES).streamQuery(b.build());

		return new Iterator<QueryBatch>() {
			private QueryBatch nextBatch = null;
			private boolean drained = false;

			@Override
			public boolean hasNext() {
				if (nextBatch != null)
					return true;
				if (drained)
					return false;
				
				try {
					
					while (responseIterator.hasNext()) {
						
						QueryResult qr = responseIterator.read();
						
						int n = qr.getTotalRecordsInBatch(); // server-populated
						// Guard: some servers could omit this; fallback to list size.
						if (n == 0)
							n = qr.getRecordsCount();

						if (qr.getRecordsCount() == 0 && !qr.getIsLastBatch()) {
							// skip empty non-terminal batch
							continue;
						}

						List<Record> converted = new ArrayList<>(qr.getRecordsCount());
						for (GrpcRecord gr : qr.getRecordsList()) {
							converted.add(grpcRecordToDBRecord(gr));
						}

						nextBatch = new QueryBatch(converted, n, qr.getRunningTotalEmitted(), qr.getIsLastBatch());

						if (qr.getIsLastBatch()) {
							drained = true; // no more after this (even if server sent empty terminal)
						}
						return true;
					}

					drained = true;
					return false;
				}
				catch (Exception e) {
					
					throw new RuntimeException("Interrupted while waiting for stream", e);
				}				
			}

			@Override
			public QueryBatch next() {
				if (!hasNext())
					throw new NoSuchElementException();
				QueryBatch out = nextBatch;
				nextBatch = null;
				return out;
			}
		};
	}

	public Iterator<GrpcRecord> queryStream(String database, String sql, Map<String, Object> params, int batchSize,
			StreamQueryRequest.RetrievalMode mode, TransactionContext tx, long timeoutMs) {

		var reqB = StreamQueryRequest.newBuilder().setDatabase(database).setQuery(sql)
				.putAllParameters(convertParamsToGrpcValue(params)).setCredentials(buildCredentials()).setBatchSize(batchSize > 0 ? batchSize : 100)
				.setRetrievalMode(mode);

		if (tx != null)
			reqB.setTransaction(tx);

		final BlockingClientCall<?, QueryResult> it = blockingStub.withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS)
				.streamQuery(reqB.build());

		return new Iterator<>() {
			
			private Iterator<GrpcRecord> curr = Collections.emptyIterator();

			public boolean hasNext() {
				
				if (curr.hasNext())
					return true;

				try {
					
					if (it.hasNext()) {
					
						curr = it.read().getRecordsList().iterator();
						return hasNext();
					}
				}
				catch (InterruptedException e) {
					throw new RuntimeException("Interrupted while waiting for stream", e);
				}
				catch (StatusException e) {
					throw new RuntimeException("Stream query failed: " + e.getStatus().getDescription());
				}

				return false;
			}

			public GrpcRecord next() {
				if (!hasNext())
					throw new NoSuchElementException();
				return curr.next();
			}
		};
	}

	public Iterator<QueryBatch> queryStreamBatches(String passLabel, String sql, Map<String, Object> params, int batchSize,
			StreamQueryRequest.RetrievalMode mode, TransactionContext tx, long timeoutMs) {

		var reqB = StreamQueryRequest.newBuilder().setDatabase(getName()).setQuery(sql)
				.putAllParameters(convertParamsToGrpcValue(params)).setCredentials(buildCredentials()).setBatchSize(batchSize > 0 ? batchSize : 100)
				.setRetrievalMode(mode);

		if (tx != null)
			reqB.setTransaction(tx);

		final BlockingClientCall<?, QueryResult> respIter = blockingStub.withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS)
				.streamQuery(reqB.build());

		return new Iterator<>() {
			
			public boolean hasNext() {
				
				try {
					return respIter.hasNext();
				}
				catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
				catch (StatusException e) {
					throw new RuntimeException(e);					
				}
			}

			public QueryBatch next() {

				QueryResult qr;
				
				try {
					qr = respIter.read();
					List<Record> converted = new ArrayList<>(qr.getRecordsCount());
					
					for (GrpcRecord gr : qr.getRecordsList()) {
						converted.add(grpcRecordToDBRecord(gr));
					}

					return new QueryBatch(converted, qr.getTotalRecordsInBatch(), // int totalInBatch
							qr.getRunningTotalEmitted(), // long runningTotal
							qr.getIsLastBatch() // boolean lastBatch
					);
				}
				catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
				catch (StatusException e) {
					throw new RuntimeException(e);	
				}				
			}
		};
	}

	public String createVertex(String cls, Map<String, Object> props, long timeoutMs) {
		// Build the nested proto GrpcRecord payload
		GrpcRecord recMsg = GrpcRecord.newBuilder().putAllProperties(convertParamsToGrpcValue(props))
				.build();

		// Build the Create request
		CreateRecordRequest req = CreateRecordRequest.newBuilder().setDatabase(getName())
				.setType(cls).setRecord(recMsg) // <<<< NESTED RECORD (not top-level properties)
				.setCredentials(buildCredentials()).build();

		// Call RPC
		CreateRecordResponse res;

		try {
			res = blockingStub.withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS).createRecord(req);
			// Response carries new RID string
			return res.getRid(); // e.g., "#12:0"
		} catch (StatusException e) {
			throw new RuntimeException("Failed to create vertex", e);
		}
	}

	public boolean updateRecord(String rid, Map<String, Object> props, long timeoutMs) {
		PropertiesUpdate partial = PropertiesUpdate.newBuilder()
				.putAllProperties(convertParamsToGrpcValue(props))
				.build();

		return updateRecord(rid, partial, timeoutMs);
	}

	public boolean updateRecord(String rid, Record dbRecord, long timeoutMs) {
		GrpcRecord record = ProtoUtils.toProtoRecord(dbRecord);
		return updateRecordFull(rid, record, timeoutMs);
	}

	private boolean updateRecord(String rid, PropertiesUpdate partial, long timeoutMs) {
		// Build the Update request using setPartial for partial update
		UpdateRecordRequest req = UpdateRecordRequest.newBuilder().setDatabase(getName())
				.setRid(rid).setPartial(partial)
				.setTransaction(TransactionContext.newBuilder().setBegin(true).setCommit(true))
				.setCredentials(buildCredentials()).build();

		// Call RPC
		UpdateRecordResponse res;

		try {
			res = blockingStub.withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS).updateRecord(req);
			// Choose the flag your proto defines. Most builds expose getSuccess().
			return res.getSuccess();
		} catch (StatusException e) {
			throw new RuntimeException("Failed to update record", e);
		}
	}

	private boolean updateRecordFull(String rid, GrpcRecord record, long timeoutMs) {
		// Build the Update request using setRecord for full replacement
		UpdateRecordRequest req = UpdateRecordRequest.newBuilder().setDatabase(getName())
				.setRid(rid).setRecord(record)
				.setTransaction(TransactionContext.newBuilder().setBegin(true).setCommit(true))
				.setCredentials(buildCredentials()).build();

		// Call RPC
		UpdateRecordResponse res;

		try {
			res = blockingStub.withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS).updateRecord(req);
			return res.getSuccess();
		} catch (StatusException e) {
			throw new RuntimeException("Failed to update record", e);
		}
	}
	
	public boolean deleteRecord(String rid, long timeoutMs) {
		var req = DeleteRecordRequest.newBuilder().setDatabase(getName()).setRid(rid).setCredentials(buildCredentials())
				.build();
		DeleteRecordResponse res;

		try {
			res = blockingStub.withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS).deleteRecord(req);
			return res.getDeleted();
		} catch (StatusException e) {
			throw new RuntimeException("Failed to delete record", e);
		}
	}

	@Override
	public RemoteTransactionExplicitLock acquireLock() {
		// Need gRPC-specific implementation
		if (explicitLock == null)
			explicitLock = new RemoteGrpcTransactionExplicitLock(this);

		return explicitLock;
	}

	@Override
	public long countBucket(final String bucketName) {
		checkDatabaseIsOpen();
		stats.countBucket.incrementAndGet();
		ResultSet result = query("sql", "select count(*) as count from bucket:" + bucketName);
		if (result.hasNext()) {
			Number count = result.next().getProperty("count");
			return count != null ? count.longValue() : 0;
		}
		return 0;
	}

	@Override
	public long countType(final String typeName, final boolean polymorphic) {
		checkDatabaseIsOpen();
		stats.countType.incrementAndGet();
		final String appendix = polymorphic ? "" : " where @type = '" + typeName + "'";
		ResultSet result = query("sql", "select count(*) as count from " + typeName + appendix);
		if (result.hasNext()) {
			Number count = result.next().getProperty("count");
			return count != null ? count.longValue() : 0;
		}
		return 0;
	}

	@Override
	public Record lookupByRID(final RID rid) {
		return lookupByRID(rid, true);
	}
	
	@Override
	public Record lookupByRID(final RID rid, final boolean loadContent) {
		checkDatabaseIsOpen();
		stats.readRecord.incrementAndGet();

		if (rid == null)
			throw new IllegalArgumentException("Record is null");

		LookupByRidRequest request = LookupByRidRequest.newBuilder().setDatabase(getName()).setRid(rid.toString())
				.setCredentials(buildCredentials()).build();

		try {
			LookupByRidResponse response = blockingStub.withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS).lookupByRid(request);

			if (!response.getFound()) {
				throw new RecordNotFoundException("Record " + rid + " not found", rid);
			}
			return grpcRecordToDBRecord(response.getRecord());
		} 
		catch (StatusRuntimeException e) {
			handleGrpcException(e);
			return null;
		} 
		catch (StatusException e) {
			handleGrpcException(e);
			return null;
		}
	}
	
	@Override
	public boolean existsRecord(RID rid) {
		stats.existsRecord.incrementAndGet();
		if (rid == null)
			throw new IllegalArgumentException("Record is null");
		try {
			return lookupByRID(rid, false) != null;
		} catch (RecordNotFoundException e) {
			return false;
		}
	}

	public InsertSummary insertBulk(final InsertOptions options,
			final List<GrpcRecord> protoRows, final long timeoutMs) {

		// Ensure options carry DB + credentials as the server expects
		InsertOptions.Builder ob = options.toBuilder();

		if (options.getDatabase() == null || options.getDatabase().isEmpty()) {
			ob.setDatabase(getName()); // your wrapper's DB name
		}

		boolean missingCreds = !options.hasCredentials() || options.getCredentials().getUsername().isEmpty();
		if (missingCreds) {
			ob.setCredentials(buildCredentials()); // your existing helper used by queries
		}

		InsertOptions newOptions = ob.build();

		BulkInsertRequest req = BulkInsertRequest.newBuilder().setOptions(newOptions)
				.addAllRows(protoRows).build();

		try {
			return blockingStub.withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS).bulkInsert(req);
		} catch (StatusException e) {
			throw new RuntimeException("insertBulk() -> failed: " + e.getStatus());
		}
	}

	// Convenience overload that accepts domain rows (convert first)
	public InsertSummary insertBulkAsListOfMaps(final InsertOptions options,
			final List<Map<String, Object>> rows, final long timeoutMs) {

		List<GrpcRecord> protoRows = rows.stream().map(this::toProtoRecordFromMap) // your converter
				.collect(java.util.stream.Collectors.toList());

		return insertBulk(options, protoRows, timeoutMs);
	}

	public InsertSummary ingestStream(final InsertOptions options,
			final List<GrpcRecord> protoRows, final int chunkSize, final long timeoutMs)
			throws InterruptedException {

		final java.util.concurrent.CountDownLatch done = new java.util.concurrent.CountDownLatch(1);
		final java.util.concurrent.atomic.AtomicReference<InsertSummary> summaryRef = new java.util.concurrent.atomic.AtomicReference<>();

		// Ensure options carry DB + credentials as the server expects
		InsertOptions.Builder ob = options.toBuilder();

		if (options.getDatabase() == null || options.getDatabase().isEmpty()) {
			ob.setDatabase(getName()); // your wrapper's DB name
		}

		boolean missingCreds = !options.hasCredentials() || options.getCredentials().getUsername().isEmpty();

		if (missingCreds) {
			ob.setCredentials(buildCredentials()); // your existing helper used by queries
		}

		StreamObserver<InsertSummary> resp = new StreamObserver<>() {
			@Override
			public void onNext(InsertSummary value) {
				summaryRef.set(value);
			}

			@Override
			public void onError(Throwable t) {
				done.countDown();
			}

			@Override
			public void onCompleted() {
				done.countDown();
			}
		};

		// Use the write service async stub, per-call deadline
		var stub = this.asyncStub.withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS);

		final StreamObserver<InsertChunk> req = stub.insertStream(resp);

		// Stream chunks
		final String sessionId = "sess-" + System.nanoTime();

		long seq = 1;

		for (int i = 0; i < protoRows.size(); i += chunkSize) {
			final int end = Math.min(i + chunkSize, protoRows.size());

			final InsertChunk chunk = InsertChunk.newBuilder().setSessionId(sessionId)
					.setOptions(ob.build()).setChunkSeq(seq++).addAllRows(protoRows.subList(i, end)).build();

			req.onNext(chunk);
		}

		req.onCompleted();

		done.await(timeoutMs + 5_000, TimeUnit.MILLISECONDS);

		final InsertSummary s = summaryRef.get();
		return (s != null) ? s : InsertSummary.newBuilder().setReceived(protoRows.size()).build();
	}

	// Convenience overload
	public InsertSummary ingestStreamAsListOfMaps(final InsertOptions options,
			final List<Map<String, Object>> rows, final int chunkSize, final long timeoutMs) throws InterruptedException {

		List<GrpcRecord> protoRows = rows.stream().map(this::toProtoRecordFromMap)
				.collect(java.util.stream.Collectors.toList());

		return ingestStream(options, protoRows, chunkSize, timeoutMs);
	}

	/**
	 * Core implementation of InsertBidirectional ingest
	 */
	private InsertSummary ingestBidiCore(final List<?> rows,
			final InsertOptions options, final int chunkSize, final int maxInflight, final long timeoutMs,
			final java.util.function.Function<Object, GrpcRecord> mapper) throws InterruptedException {

		// 1) Ensure options carry DB + credentials the server expects
		final InsertOptions.Builder ob = options.toBuilder();

		if (options.getDatabase() == null || options.getDatabase().isEmpty()) {
			ob.setDatabase(getName());
		}

		final boolean missingCreds = !options.hasCredentials() || options.getCredentials().getUsername().isEmpty();

		if (missingCreds) {
			ob.setCredentials(buildCredentials());
		}

		final InsertOptions effectiveOpts = ob.build();

		// 2) Pre-map rows → proto once (outside the observer)
		final List<GrpcRecord> protoRows = rows.stream().map(mapper)
				.collect(java.util.stream.Collectors.toList());

		// 3) Streaming state
		final String sessionId = "sess-" + System.nanoTime();
		final java.util.concurrent.CountDownLatch done = new java.util.concurrent.CountDownLatch(1);
		final java.util.concurrent.atomic.AtomicLong seq = new java.util.concurrent.atomic.AtomicLong(1);
		final java.util.concurrent.atomic.AtomicInteger cursor = new java.util.concurrent.atomic.AtomicInteger(0);
		final java.util.concurrent.atomic.AtomicInteger sent = new java.util.concurrent.atomic.AtomicInteger(0);
		final java.util.concurrent.atomic.AtomicInteger acked = new java.util.concurrent.atomic.AtomicInteger(0);
		final java.util.concurrent.atomic.AtomicReference<InsertSummary> committed = new java.util.concurrent.atomic.AtomicReference<>();
		final List<BatchAck> acks = java.util.Collections.synchronizedList(new ArrayList<>());

		// small holder to access req inside helpers
		final java.util.concurrent.atomic.AtomicReference<ClientCallStreamObserver<InsertRequest>> observerRef = 
				new java.util.concurrent.atomic.AtomicReference<>();

		final java.util.concurrent.atomic.AtomicBoolean commitSent = new java.util.concurrent.atomic.AtomicBoolean(false);
		final java.util.concurrent.ScheduledExecutorService scheduler = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(r -> {
			Thread t = new Thread(r, "grpc-ack-grace-timer");
			t.setDaemon(true);
			return t;
		});
		final long ackGraceMillis = Math.min(Math.max(timeoutMs / 10, 1_000L), 10_000L); // e.g., 1s..10s window
		final Object timerLock = new Object();
		final java.util.concurrent.atomic.AtomicReference<java.util.concurrent.ScheduledFuture<?>> ackGraceFuture = new java.util.concurrent.atomic.AtomicReference<>();

		// helper: schedule COMMIT if still waiting
		final Runnable sendCommitIfNeeded = () -> {
			if (commitSent.compareAndSet(false, true)) {
				try {
					if (observerRef.get() != null) {
						observerRef.get().onNext(InsertRequest.newBuilder()
								.setCommit(Commit.newBuilder().setSessionId(sessionId)).build());
						observerRef.get().onCompleted();
					}
				} catch (Throwable ignore) {
					/* best effort */ }
			}
		};

		// helper: (re)arm the grace timer
		final Runnable armAckGraceTimer = () -> {
			synchronized (timerLock) {
				var prev = ackGraceFuture.getAndSet(null);
				if (prev != null)
					prev.cancel(false);
				// only arm if we've sent all chunks but acks are still pending
				if (cursor.get() >= protoRows.size() && acked.get() < sent.get() && !commitSent.get()) {
					var fut = scheduler.schedule(sendCommitIfNeeded, ackGraceMillis, TimeUnit.MILLISECONDS);
					ackGraceFuture.set(fut);
				}
			}
		};

		// helper: cancel timer (e.g., once all ACKed or after COMMIT observed)
		final Runnable cancelAckGraceTimer = () -> {
			synchronized (timerLock) {
				var prev = ackGraceFuture.getAndSet(null);
				if (prev != null)
					prev.cancel(false);
			}
		};

		final ClientResponseObserver<InsertRequest, InsertResponse> observer = new ClientResponseObserver<>() {

			ClientCallStreamObserver<InsertRequest> req;
			volatile boolean started = false;

			@Override
			public void beforeStart(ClientCallStreamObserver<InsertRequest> r) {
				this.req = r;
				observerRef.set(r); // <-- make req visible to helper
				r.disableAutoInboundFlowControl();
				r.setOnReadyHandler(this::drain);
			}

			private void drain() {
				if (!req.isReady())
					return;

				if (!started) {
					req.onNext(InsertRequest.newBuilder()
							.setStart(Start.newBuilder().setOptions(effectiveOpts)).build());
					
					started = true;
					
					// Now that the call is started, it's safe to pull the first response
					req.request(1);
				}

				while (req.isReady() && (sent.get() - acked.get()) < maxInflight) {
					final int start = cursor.get();
					if (start >= protoRows.size())
						break;

					final int end = Math.min(start + chunkSize, protoRows.size());
					final var slice = protoRows.subList(start, end);

					final var chunk = InsertChunk.newBuilder().setSessionId(sessionId)
							.setChunkSeq(seq.getAndIncrement()).addAllRows(slice).build();

					req.onNext(InsertRequest.newBuilder().setChunk(chunk).build());
					cursor.set(end);
					sent.incrementAndGet();
				}

				// If all chunks sent:
				if (cursor.get() >= protoRows.size()) {
					if (acked.get() >= sent.get()) {
						// all acked → commit immediately
						sendCommitIfNeeded.run();
					} else {
						// not all acked → (re)arm grace timer; if last ACK never arrives, timer will COMMIT
						armAckGraceTimer.run();
					}
				}
			}

			@Override
			public void onNext(InsertResponse v) {
				switch (v.getMsgCase()) {
				case STARTED -> {
					/* ok */ }
				case BATCH_ACK -> {
					acks.add(v.getBatchAck());
					acked.incrementAndGet();
					// Each ACK may free capacity: push more & manage timer
					drain();
					if (cursor.get() >= protoRows.size()) {
						if (acked.get() >= sent.get()) {
							cancelAckGraceTimer.run();
							sendCommitIfNeeded.run();
						} else {
							armAckGraceTimer.run();
						}
					}
				}
				case COMMITTED -> {
					committed.set(v.getCommitted().getSummary());
					cancelAckGraceTimer.run();
				}
				case MSG_NOT_SET -> {
					/* ignore */ }
				}
				req.request(1); // continue pulling responses
			}

			@Override
			public void onError(Throwable t) {
				cancelAckGraceTimer.run();
				done.countDown();
			}

			@Override
			public void onCompleted() {
				cancelAckGraceTimer.run();
				done.countDown();
			}
		};
		// 4) Kick off the bidi call with deadline

		try {
			asyncStub.withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS).insertBidirectional(observer);			
			boolean finished = done.await(timeoutMs + 5_000, TimeUnit.MILLISECONDS);
		} finally {
			scheduler.shutdownNow();
		}

		// 6) Prefer COMMITTED summary if present; otherwise aggregate ACKs
		final InsertSummary finalSummary = committed.get();
		if (finalSummary != null)
			return finalSummary;

		final long ins = acks.stream().mapToLong(BatchAck::getInserted).sum();
		final long upd = acks.stream().mapToLong(BatchAck::getUpdated).sum();
		final long ign = acks.stream().mapToLong(BatchAck::getIgnored).sum();
		final long fail = acks.stream().mapToLong(BatchAck::getFailed).sum();

		return InsertSummary.newBuilder().setReceived(protoRows.size()).setInserted(ins).setUpdated(upd).setIgnored(ign)
				.setFailed(fail).build();
	}

	/**
	 * Pushes domain {@code com.arcadedb.database.Record} rows via InsertBidirectional with per-batch ACKs.
	 */
	public InsertSummary ingestBidi(final List<com.arcadedb.database.Record> rows,
			final InsertOptions opts, final int chunkSize, final int maxInflight, final long timeoutMs)
			throws InterruptedException {

		return ingestBidiCore(rows, opts, chunkSize, maxInflight, timeoutMs,
				(Object o) -> toProtoRecordFromDbRecord((com.arcadedb.database.Record) o));
	}

	public InsertSummary ingestBidi(final List<com.arcadedb.database.Record> rows,
			final InsertOptions opts, final int chunkSize, final int maxInflight) throws InterruptedException {

		return ingestBidiCore(rows, opts, chunkSize, maxInflight, /* timeoutMs */ 5 * 60_000L,
				(Object o) -> toProtoRecordFromDbRecord((com.arcadedb.database.Record) o));
	}

	/**
	 * Pushes map-shaped rows (property map per row) via InsertBidirectional with per-batch ACKs.
	 */
	public InsertSummary ingestBidi(final InsertOptions options,
			final List<Map<String, Object>> rows, final int chunkSize, final int maxInflight) throws InterruptedException {

		return ingestBidiCore(rows, options, chunkSize, maxInflight, /* timeoutMs */ 5 * 60_000L,
				(Object o) -> toProtoRecordFromMap((Map<String, Object>) o));
	}

	public InsertSummary ingestBidi(final InsertOptions options,
			final List<Map<String, Object>> rows, final int chunkSize, final int maxInflight, final long timeoutMs)
			throws InterruptedException {

		return ingestBidiCore(rows, options, chunkSize, maxInflight, timeoutMs,
				(Object o) -> toProtoRecordFromMap((Map<String, Object>) o));
	}

	// Map -> GrpcRecord
	private GrpcRecord toProtoRecordFromMap(Map<String, Object> row) {
		GrpcRecord.Builder b = GrpcRecord.newBuilder();
		row.forEach((k, v) -> b.putProperties(k, objectToGrpcValue(v)));
		return b.build();
	}

	// Domain Record (storage) -> GrpcRecord
	private GrpcRecord toProtoRecordFromDbRecord(com.arcadedb.database.Record rec) {
		// Use ProtoUtils for proper conversion
		return ProtoUtils.toProtoRecord(rec);
	}

	// Query Result -> GrpcRecord
	private GrpcRecord toProtoRecordFromResult(Result res) {
		GrpcRecord.Builder b = GrpcRecord.newBuilder();
		for (String name : res.getPropertyNames()) {
			Object v = res.getProperty(name);
			b.putProperties(name, objectToGrpcValue(v));
		}
		return b.build();
	}

	// Convert Java object -> GrpcValue (extend as needed)
	private GrpcValue objectToGrpcValue(Object v) {
		return ProtoUtils.toGrpcValue(v);
	}

	// google.protobuf.Value -> Java (mirror of your toProtoValue)
	private static Object fromProtoValue(Value v) {
		if (v == null)
			return null;
		switch (v.getKindCase()) {
		case NULL_VALUE:
			return null;
		case BOOL_VALUE:
			return v.getBoolValue();
		case NUMBER_VALUE:
			return v.getNumberValue();
		case STRING_VALUE:
			return v.getStringValue();
		case LIST_VALUE: {
			List<Object> out = new ArrayList<>();
			for (Value item : v.getListValue().getValuesList()) {
				out.add(fromProtoValue(item));
			}
			return out;
		}
		case STRUCT_VALUE: {
			Map<String, Object> m = new java.util.LinkedHashMap<>();
			v.getStructValue().getFieldsMap().forEach((k, vv) -> m.put(k, fromProtoValue(vv)));
			return m;
		}
		case KIND_NOT_SET:
		default:
			return null;
		}
	}

	public DatabaseCredentials buildCredentials() {
		return DatabaseCredentials.newBuilder().setUsername(getUserName()).setPassword(getUserPassword()).build();
	}

	private TransactionIsolation mapIsolationLevel(Database.TRANSACTION_ISOLATION_LEVEL level) {
		switch (level) {
//      case READ_UNCOMMITTED:
//        return TransactionIsolation.READ_UNCOMMITTED;
		case READ_COMMITTED:
			return TransactionIsolation.READ_COMMITTED;
		case REPEATABLE_READ:
			return TransactionIsolation.REPEATABLE_READ;
//      case SERIALIZABLE:
//        return TransactionIsolation.SERIALIZABLE;
		default:
			return TransactionIsolation.READ_COMMITTED;
		}
	}

	// ---- TX helpers -------------------------------------------------------------
	private static TransactionContext txBeginCommit() {
		return TransactionContext.newBuilder().setBegin(true).setCommit(true).build();
	}

	private static TransactionContext txNone() {
		return TransactionContext.getDefaultInstance();
	}

	// Optional: language defaulting (server defaults to "sql" too)
	private static String langOrDefault(String language) {
		return (language == null || language.isEmpty()) ? "sql" : language;
	}

	private Iterator<Record> streamQuery(final String query) {
		StreamQueryRequest request = StreamQueryRequest.newBuilder().setDatabase(getName()).setQuery(query).setCredentials(buildCredentials())
				.setBatchSize(100).build();

		final BlockingClientCall<?, QueryResult> responseIterator = blockingStub.withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS)
				.streamQuery(request);

		return new Iterator<Record>() {
			private Iterator<GrpcRecord> currentBatch = Collections.emptyIterator();

			@Override
			public boolean hasNext() {
				
				if (currentBatch.hasNext()) {
					return true;
				}

				try {
					if (responseIterator.hasNext()) {

						QueryResult result;
							result = responseIterator.read();
							currentBatch = result.getRecordsList().iterator();
							return currentBatch.hasNext();
					}
				}
				catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
				catch (StatusException e) {
					throw new RuntimeException(e);
				}

				return false;
			}

			@Override
			public Record next() {
				if (!hasNext()) {
					throw new NoSuchElementException();
				}
				return grpcRecordToDBRecord(currentBatch.next());
			}
		};
	}

	private ResultSet createGrpcResultSet(ExecuteQueryResponse response) {
		InternalResultSet resultSet = new InternalResultSet();
		for (QueryResult queryResult : response.getResultsList()) {
			for (GrpcRecord record : queryResult.getRecordsList()) {
				resultSet.add(grpcRecordToResult(record));
			}
		}
		return resultSet;
	}

	private Result grpcRecordToResult(GrpcRecord grpcRecord) {
		Record record = grpcRecordToDBRecord(grpcRecord);
		if (record == null) {
			Map<String, Object> properties = new HashMap<>();
			grpcRecord.getPropertiesMap().forEach((k, v) -> properties.put(k, grpcValueToObject(v)));
			return new ResultInternal(properties);
		}
		return new ResultInternal(record);
	}

	private Record grpcRecordToDBRecord(GrpcRecord grpcRecord) {
		
		Map<String, Object> map = new HashMap<>();

		// Convert properties
		grpcRecord.getPropertiesMap().forEach((k, v) -> map.put(k, grpcValueToObject(v)));

		// Add metadata
		map.put("@rid", grpcRecord.getRid());
		map.put("@type", grpcRecord.getType());
		map.put("@cat", mapRecordType(grpcRecord));

		String cat = (String) map.get("@cat");
		
		if (cat == null) {
			
			return null;
		}
		
		switch (cat) {
		case "d":
			return new RemoteImmutableDocument(this, map);
		case "v":
			return new RemoteImmutableVertex(this, map);
		case "e":
			return new RemoteImmutableEdge(this, map);
		default:
			return null;
		}
	}

	private String mapRecordType(GrpcRecord grpcRecord) {
		
		// Determine record category from type name
		String typeName = grpcRecord.getType();

		// Check schema to determine actual type
		try {
			
			if (getSchema().existsType(typeName)) {
				
				Object type = getSchema().getType(typeName);
				
				if (type instanceof com.arcadedb.schema.VertexType) {				
					return "v";
				} 
				else if (type instanceof com.arcadedb.schema.EdgeType) {
					return "e";
				}
				else if (type instanceof com.arcadedb.schema.DocumentType) {
					return "d";
				}
				else {
                    return null;
                }
			}
			else {
				
				return null;
			}
		} 
		catch (Exception e) {
			// Fall back to name-based detection
		}

		return null;
		
//		// Fall back to name-based detection
//		if (typeName.contains("Vertex") || typeName.startsWith("V_")) {
//			return "v";
//		} else if (typeName.contains("Edge") || typeName.startsWith("E_")) {
//			return "e";
//		} else {
//			return "d";
//		}
	}

	private Map<String, GrpcValue> convertParamsToGrpcValue(Map<String, Object> params) {
		Map<String, GrpcValue> grpcParams = new HashMap<>();
		
		for (Map.Entry<String, Object> entry : params.entrySet()) {
			GrpcValue value = objectToGrpcValue(entry.getValue());
			grpcParams.put(entry.getKey(), value);
		}
		
		return grpcParams;
	}

	private Object grpcValueToObject(GrpcValue grpcValue) {
		return ProtoUtils.fromGrpcValue(grpcValue);
	}

	private void handleGrpcException(StatusRuntimeException e) {
		Status status = e.getStatus();
		switch (status.getCode()) {
		case NOT_FOUND:
			throw new RecordNotFoundException(status.getDescription(), null);
		case ALREADY_EXISTS:
			throw new DuplicatedKeyException("", "", null);
		case ABORTED:
			throw new ConcurrentModificationException(status.getDescription());
		case DEADLINE_EXCEEDED:
			throw new TimeoutException(status.getDescription());
		case PERMISSION_DENIED:
			throw new SecurityException(status.getDescription());
		case UNAVAILABLE:
			throw new NeedRetryException(status.getDescription());
		default:
			throw new RemoteException("gRPC error: " + status.getDescription(), e);
		}
	}

	private void handleGrpcException(StatusException e) {
		Status status = e.getStatus();
		switch (status.getCode()) {
		case NOT_FOUND:
			throw new RecordNotFoundException(status.getDescription(), null);
		case ALREADY_EXISTS:
			throw new DuplicatedKeyException("", "", null);
		case ABORTED:
			throw new ConcurrentModificationException(status.getDescription());
		case DEADLINE_EXCEEDED:
			throw new TimeoutException(status.getDescription());
		case PERMISSION_DENIED:
			throw new SecurityException(status.getDescription());
		case UNAVAILABLE:
			throw new NeedRetryException(status.getDescription());
		default:
			throw new RemoteException("gRPC error: " + status.getDescription(), e);
		}
	}

	/**
	 * Extended version with compression support
	 */
	public static class RemoteGrpcDatabaseWithCompression extends RemoteGrpcDatabase {

		public RemoteGrpcDatabaseWithCompression(String server, int grpcPort, int httpPort, String databaseName, String userName,
				String userPassword, ContextConfiguration configuration) {
			super(server, grpcPort, httpPort, databaseName, userName, userPassword, configuration);
		}

		@Override
		protected ManagedChannel createChannel(String server, int port) {

			return ManagedChannelBuilder.forAddress(server, port).usePlaintext() // No TLS/SSL
					.compressorRegistry(CompressorRegistry.getDefaultInstance()).decompressorRegistry(DecompressorRegistry.getDefaultInstance())
					.maxInboundMessageSize(100 * 1024 * 1024) // 100MB max message size
					.keepAliveTime(30, TimeUnit.SECONDS) // Keep-alive configuration
					.keepAliveTimeout(10, TimeUnit.SECONDS).keepAliveWithoutCalls(true).build();
		}
	}

	// Add the missing RemoteGrpcTransactionExplicitLock class reference
	private static class RemoteGrpcTransactionExplicitLock extends RemoteTransactionExplicitLock {
		public RemoteGrpcTransactionExplicitLock(RemoteGrpcDatabase database) {
			super(database);
		}
	}
}