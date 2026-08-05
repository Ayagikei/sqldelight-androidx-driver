package com.eygraber.sqldelight.androidx.driver

import androidx.sqlite.SQLiteConnection
import androidx.sqlite.execSQL
import app.cash.sqldelight.db.QueryResult
import com.eygraber.sqldelight.androidx.driver.AndroidxSqliteConcurrencyModel.MultipleReadersSingleWriter
import com.eygraber.sqldelight.androidx.driver.AndroidxSqliteConcurrencyModel.SingleReaderWriter
import kotlinx.atomicfu.locks.ReentrantLock
import kotlinx.atomicfu.locks.withLock
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlin.concurrent.Volatile

internal interface ConnectionPool : AutoCloseable {
  fun acquireWriterConnection(): SQLiteConnection
  fun releaseWriterConnection()
  fun invalidateWriterConnection(cause: Throwable)
  fun acquireReaderConnection(): SQLiteConnection
  fun releaseReaderConnection(connection: SQLiteConnection)
  fun <R> setJournalMode(
    executeStatement: (SQLiteConnection) -> QueryResult.Value<R>,
  ): QueryResult.Value<R>
}

internal inline fun <R> ConnectionPool.withWriterConnection(
  block: SQLiteConnection.() -> R,
): R {
  val connection = acquireWriterConnection()
  try {
    return connection.block()
  } finally {
    releaseWriterConnection()
  }
}

internal class AndroidxDriverConnectionPool(
  private val connectionFactory: AndroidxSqliteConnectionFactory,
  nameProvider: () -> String,
  private val isFileBased: Boolean,
  private val configuration: AndroidxSqliteConfiguration,
  private val closeConnection: (SQLiteConnection) -> Unit = { connection -> connection.close() },
  private val onReaderJournalModeGateWait: () -> Unit = {},
  private val onReaderConnectionWait: () -> Unit = {},
) : ConnectionPool {
  private data class ReaderSQLiteConnection(
    val isCreated: Boolean,
    val connection: Lazy<SQLiteConnection>,
  )

  private val name by lazy { nameProvider() }

  private val writerConnectionHolder = WriterConnectionHolder(
    connectionFactory = connectionFactory,
    nameProvider = { name },
    configuration = configuration,
    closeConnection = closeConnection,
  )
  private val writerConnection: SQLiteConnection get() = writerConnectionHolder.connection

  private val writerMutex = Mutex()

  private val journalModeLock = ReentrantLock()
  private val closeLock = ReentrantLock()

  private var pendingCloseReaders: MutableList<ReaderSQLiteConnection>? = null
  private val pendingJournalCloseConnections = mutableListOf<SQLiteConnection>()
  private var isWriterClosed = false
  private var isClosed = false

  @Volatile
  private var terminalFailure: Throwable? = null

  private val journalModeConcurrencyModel = when {
    isFileBased -> configuration.concurrencyModel as? MultipleReadersSingleWriter
    else -> null
  }

  @Volatile
  private var concurrencyModel = when {
    journalModeConcurrencyModel != null -> SingleReaderWriter
    isFileBased -> configuration.concurrencyModel
    else -> SingleReaderWriter
  }
  private var isReaderTopologyInitialized = journalModeConcurrencyModel == null

  private val readerChannel = Channel<ReaderSQLiteConnection>(capacity = Channel.UNLIMITED)

  init {
    if(isReaderTopologyInitialized) populateReaderConnectionChannel()
  }

  /**
   * Acquires the writer connection, blocking if it's currently in use.
   * @return The writer SQLiteConnection
   */
  override fun acquireWriterConnection(): SQLiteConnection {
    checkOperational()
    journalModeLock.lock()
    try {
      return runBlocking {
        writerMutex.lock()
        try {
          checkOperational()
          writerConnection.also {
            activateInitialReaderTopology(writerConnectionHolder.initialJournalMode)
          }
        } catch(t: Throwable) {
          writerMutex.unlock()
          throw t
        }
      }
    }
    finally {
      journalModeLock.unlock()
    }
  }

  /**
   * Releases the writer connection (mutex unlocks automatically).
   */
  override fun releaseWriterConnection() {
    writerMutex.unlock()
  }

  override fun invalidateWriterConnection(cause: Throwable) {
    if(terminalFailure == null) terminalFailure = cause
    try {
      writerConnectionHolder.close()
    } catch(closeFailure: Throwable) {
      if(closeFailure !== cause) cause.addSuppressed(closeFailure)
    }
  }

  /**
   * Acquires a reader connection, blocking if none are available.
   * @return A reader SQLiteConnection
   */
  override fun acquireReaderConnection(): SQLiteConnection {
    checkOperational()
    if(!journalModeLock.tryLock()) {
      onReaderJournalModeGateWait()
      journalModeLock.lock()
    }

    try {
      checkOperational()
      ensureReaderTopologyInitialized()
      return when(concurrencyModel.readerCount) {
        0 -> acquireWriterConnection()
        else -> runBlocking {
          val reader = readerChannel.tryReceive().getOrNull() ?: run {
            onReaderConnectionWait()
            readerChannel.receive()
          }
          try {
            checkOperational()
          } catch(t: Throwable) {
            readerChannel.send(reader)
            throw t
          }
          try {
            reader.connection.value
          } catch(t: Throwable) {
            readerChannel.send(createUnopenedReaderConnection())
            throw t
          }
        }
      }
    } finally {
      journalModeLock.unlock()
    }
  }

  /**
   * Releases a reader connection back to the pool.
   * @param connection The SQLiteConnection to release
   */
  override fun releaseReaderConnection(connection: SQLiteConnection) {
    when(concurrencyModel.readerCount) {
      0 -> releaseWriterConnection()
      else -> runBlocking {
        readerChannel.send(
          ReaderSQLiteConnection(
            isCreated = true,
            lazy { connection },
          ),
        )
      }
    }
  }

  override fun <R> setJournalMode(
    executeStatement: (SQLiteConnection) -> QueryResult.Value<R>,
  ): QueryResult.Value<R> = journalModeLock.withLock {
    checkOperational()
    var isWriterAcquired = false
    try {
      ensureReaderTopologyInitialized()
      closeAllReaderConnections()

      val writer = acquireWriterConnection()
      isWriterAcquired = true
      val isForeignKeyConstraintsEnabled = writer.readForeignKeyConstraintsEnabled()
      var queryResult: QueryResult.Value<R>? = null
      var journalModeFailure: Throwable? = null

      fun recordJournalModeFailure(failure: Throwable) {
        val primaryFailure = journalModeFailure
        if(primaryFailure == null) {
          journalModeFailure = failure
        } else if(failure !== primaryFailure) {
          primaryFailure.addSuppressed(failure)
        }
      }

      try {
        queryResult = executeStatement(writer)
      } catch(t: Throwable) {
        recordJournalModeFailure(t)
      }

      queryResult?.let { completedQueryResult ->
        if(journalModeConcurrencyModel != null && completedQueryResult.value !is String) {
          recordJournalModeFailure(invalidJournalModeResult(completedQueryResult.value))
        }
      }

      try {
        writer.restoreForeignKeyConstraints(isForeignKeyConstraintsEnabled)
      } catch(t: Throwable) {
        recordJournalModeFailure(t)
      }

      journalModeConcurrencyModel?.let { configuredModel ->
        try {
          val actualJournalMode = writer.readJournalMode()
          concurrencyModel = configuredModel.copy(
            isWal = actualJournalMode.equals("wal", ignoreCase = true),
          )
        } catch(t: Throwable) {
          recordJournalModeFailure(t)
          concurrencyModel = SingleReaderWriter
        }
      }

      journalModeFailure?.let { throw it }
      return requireNotNull(queryResult)
    } finally {
      try {
        if(isWriterAcquired) releaseWriterConnection()
      } finally {
        populateReaderConnectionChannel()
      }
    }
  }

  /**
   * Closes all connections in the pool.
   */
  override fun close() {
    closeLock.withLock {
      if(isClosed) return@withLock

      runBlocking {
        val readersToClose = pendingCloseReaders ?: prepareReadersForClose()
        var closeFailure: Throwable? = null

        fun recordCloseFailure(failure: Throwable) {
          val primaryFailure = closeFailure
          if(primaryFailure == null) {
            closeFailure = failure
          } else if(failure !== primaryFailure) {
            primaryFailure.addSuppressed(failure)
          }
        }

        writerMutex.withLock {
          if(!isWriterClosed) {
            try {
              writerConnectionHolder.close()
            } catch(t: Throwable) {
              recordCloseFailure(t)
            }
            isWriterClosed = writerConnectionHolder.isClosed
          }
        }

        val readerIterator = readersToClose.iterator()
        while(readerIterator.hasNext()) {
          val reader = readerIterator.next()
          if(!reader.isCreated) {
            readerIterator.remove()
            continue
          }

          try {
            closeConnection(reader.connection.value)
            readerIterator.remove()
          } catch(t: Throwable) {
            recordCloseFailure(t)
          }
        }

        val pendingJournalIterator = pendingJournalCloseConnections.iterator()
        while(pendingJournalIterator.hasNext()) {
          val connection = pendingJournalIterator.next()
          try {
            // The driver retries statement-cache cleanup before closing the pool. At this point the
            // pool only retains ownership of the physical connection whose earlier close failed.
            connection.close()
            pendingJournalIterator.remove()
          } catch(t: Throwable) {
            recordCloseFailure(t)
          }
        }

        if(isWriterClosed && readersToClose.isEmpty() && pendingJournalCloseConnections.isEmpty()) {
          isClosed = true
        }

        closeFailure?.let { throw it }
      }
    }
  }

  private suspend fun prepareReadersForClose(): MutableList<ReaderSQLiteConnection> {
    val readerCount = concurrencyModel.readerCount
    val availableReaders = mutableListOf<ReaderSQLiteConnection>()
    while(true) {
      val reader = readerChannel.tryReceive().getOrNull() ?: break
      availableReaders += reader
    }

    val outstandingReaders = readerCount - availableReaders.size
    if(outstandingReaders > 0) {
      availableReaders.forEach { readerChannel.send(it) }
      error(
        "AndroidxDriverConnectionPool.close() called while " +
          "$outstandingReaders reader connection(s) still checked out",
      )
    }

    pendingCloseReaders = availableReaders
    readerChannel.close()
    return availableReaders
  }

  private fun closeAllReaderConnections() {
    val readerCount = concurrencyModel.readerCount
    if(readerCount > 0 || pendingJournalCloseConnections.isNotEmpty()) {
      runBlocking {
        var closeFailure: Throwable? = null

        fun closeOrRetain(connection: SQLiteConnection) {
          try {
            closeConnection(connection)
          } catch(t: Throwable) {
            pendingJournalCloseConnections += connection
            val primaryFailure = closeFailure
            if(primaryFailure == null) {
              closeFailure = t
            } else if(t !== primaryFailure) {
              primaryFailure.addSuppressed(t)
            }
          }
        }

        val previouslyFailedConnections = pendingJournalCloseConnections.toList()
        pendingJournalCloseConnections.clear()
        previouslyFailedConnections.forEach(::closeOrRetain)

        repeat(readerCount) {
          val reader = readerChannel.receive()
          // only apply the pragma to connections that were already created
          if(reader.isCreated) {
            closeOrRetain(reader.connection.value)
          }
        }
        closeFailure?.let { throw it }
      }
    }
  }

  private fun populateReaderConnectionChannel() {
    repeat(concurrencyModel.readerCount) {
      readerChannel.trySend(
        createUnopenedReaderConnection(),
      )
    }
  }

  private fun ensureReaderTopologyInitialized() {
    if(isReaderTopologyInitialized) return
    acquireWriterConnection()
    releaseWriterConnection()
  }

  private fun activateInitialReaderTopology(actualJournalMode: String) {
    val configuredModel = journalModeConcurrencyModel ?: return
    if(isReaderTopologyInitialized) return

    concurrencyModel = configuredModel.copy(
      isWal = actualJournalMode.equals("wal", ignoreCase = true),
    )
    populateReaderConnectionChannel()
    isReaderTopologyInitialized = true
  }

  private fun createUnopenedReaderConnection() =
    ReaderSQLiteConnection(
      isCreated = false,
      connection = lazy {
        connectionFactory.createConnection(name)
      },
    )

  private fun checkOperational() {
    terminalFailure?.let { cause ->
      throw IllegalStateException(
        "SQLite writer is unusable after failed transaction recovery",
        cause,
      )
    }
  }
}

internal class PassthroughConnectionPool(
  private val connectionFactory: AndroidxSqliteConnectionFactory,
  nameProvider: () -> String,
  configuration: AndroidxSqliteConfiguration,
  private val onTerminalFailureSet: () -> Unit = {},
) : ConnectionPool {
  private val name by lazy { nameProvider() }

  private val delegatedConnectionHolder = WriterConnectionHolder(
    connectionFactory = connectionFactory,
    nameProvider = { name },
    configuration = configuration,
    closeConnection = { connection -> connection.close() },
  )
  private val delegatedConnection: SQLiteConnection get() = delegatedConnectionHolder.connection

  @Volatile
  private var terminalFailure: Throwable? = null

  override fun acquireWriterConnection() = getOperationalConnection()

  override fun releaseWriterConnection() {}

  override fun acquireReaderConnection() = getOperationalConnection()

  override fun releaseReaderConnection(connection: SQLiteConnection) {}

  override fun <R> setJournalMode(
    executeStatement: (SQLiteConnection) -> QueryResult.Value<R>,
  ): QueryResult.Value<R> = getOperationalConnection().executePreservingForeignKeys(executeStatement)

  override fun invalidateWriterConnection(cause: Throwable) {
    if(terminalFailure == null) terminalFailure = cause
    onTerminalFailureSet()
    try {
      delegatedConnectionHolder.close()
    } catch(closeFailure: Throwable) {
      if(closeFailure !== cause) cause.addSuppressed(closeFailure)
    }
  }

  override fun close() {
    delegatedConnectionHolder.close()
  }

  private fun getOperationalConnection(): SQLiteConnection {
    checkOperational()
    return delegatedConnection.also { checkOperational() }
  }

  private fun checkOperational() {
    terminalFailure?.let { cause ->
      throw IllegalStateException(
        "SQLite writer is unusable after failed transaction recovery",
        cause,
      )
    }
  }
}

private class WriterConnectionHolder(
  private val connectionFactory: AndroidxSqliteConnectionFactory,
  private val nameProvider: () -> String,
  configuration: AndroidxSqliteConfiguration,
  private val closeConnection: (SQLiteConnection) -> Unit,
) {
  private val lock = ReentrantLock()
  private val configuration = configuration.copy()
  private var pendingPhysicalClose: SQLiteConnection? = null
  private var isConfiguredConnectionClosed = false
  private val lazyConnection = lazy { createConfiguredConnection() }

  val connection: SQLiteConnection
    get() = lock.withLock {
      retryPendingPhysicalClose()
      lazyConnection.value.connection
    }

  val initialJournalMode: String
    get() = lock.withLock {
      retryPendingPhysicalClose()
      lazyConnection.value.journalMode
    }

  val isClosed: Boolean
    get() = lock.withLock {
      pendingPhysicalClose == null && (!lazyConnection.isInitialized() || isConfiguredConnectionClosed)
    }

  fun close() = lock.withLock {
    var closeFailure: Throwable? = null

    try {
      retryPendingPhysicalClose()
    } catch(t: Throwable) {
      closeFailure = t
    }

    if(lazyConnection.isInitialized() && !isConfiguredConnectionClosed) {
      try {
        closeConnection(lazyConnection.value.connection)
        isConfiguredConnectionClosed = true
      } catch(t: Throwable) {
        val primaryFailure = closeFailure
        if(primaryFailure == null) {
          closeFailure = t
        } else if(t !== primaryFailure) {
          primaryFailure.addSuppressed(t)
        }
      }
    }

    closeFailure?.let { throw it }
  }

  private fun createConfiguredConnection(): ConfiguredWriterConnection {
    val connection = connectionFactory.createConnection(nameProvider())
    return try {
      connection.withWriterConfiguration(configuration)
    } catch(t: Throwable) {
      try {
        closeConnection(connection)
      } catch(closeFailure: Throwable) {
        pendingPhysicalClose = connection
        if(closeFailure !== t) t.addSuppressed(closeFailure)
      }
      throw t
    }
  }

  private fun retryPendingPhysicalClose() {
    val connection = pendingPhysicalClose ?: return
    connection.close()
    pendingPhysicalClose = null
  }
}

private fun SQLiteConnection.withWriterConfiguration(
  configuration: AndroidxSqliteConfiguration,
): ConfiguredWriterConnection {
  // copy the configuration for thread safety
  return configuration.copy().let { copiedConfiguration ->
    val actualJournalMode = setJournalModeAndReadActual(copiedConfiguration.journalMode)
    execSQL("PRAGMA synchronous = ${copiedConfiguration.sync.value};")

    // this must to come after PRAGMA journal_mode while https://issuetracker.google.com/issues/447613208 is broken
    val foreignKeys = if(copiedConfiguration.isForeignKeyConstraintsEnabled) "ON" else "OFF"
    execSQL("PRAGMA foreign_keys = $foreignKeys;")

    ConfiguredWriterConnection(
      connection = this,
      journalMode = actualJournalMode,
    )
  }
}

private data class ConfiguredWriterConnection(
  val connection: SQLiteConnection,
  val journalMode: String,
)

private fun SQLiteConnection.setJournalModeAndReadActual(journalMode: SqliteJournalMode): String = prepare(
  "PRAGMA journal_mode = ${journalMode.value};",
).use { statement ->
  check(statement.step()) { "PRAGMA journal_mode returned no rows" }
  requireNotNull(statement.getText(0)) { "PRAGMA journal_mode returned null" }
}

private fun SQLiteConnection.readForeignKeyConstraintsEnabled(): Boolean = prepare(
  "PRAGMA foreign_keys;",
).use { statement ->
  statement.step()
  statement.getBoolean(0)
}

private fun SQLiteConnection.readJournalMode(): String = prepare(
  "PRAGMA journal_mode;",
).use { statement ->
  check(statement.step()) { "PRAGMA journal_mode returned no rows" }
  requireNotNull(statement.getText(0)) { "PRAGMA journal_mode returned null" }
}

private fun invalidJournalModeResult(value: Any?): IllegalStateException = IllegalStateException(
  """
  PRAGMA journal_mode is intercepted by AndroidxSqliteDriver to keep its connection pool
  in sync with the database's journal mode, which requires the query result to be a String.
  Got ${value?.let { it::class.simpleName ?: "<type unknown>" } ?: "null"} instead. Either remove the custom
  column adapter from this query, or set the journal mode via
  AndroidxSqliteConfigurableDriver.setJournalMode in onConfigure.
  """.trimIndent(),
)

private inline fun <R> SQLiteConnection.executePreservingForeignKeys(
  executeStatement: (SQLiteConnection) -> QueryResult.Value<R>,
): QueryResult.Value<R> {
  val isForeignKeyConstraintsEnabled = readForeignKeyConstraintsEnabled()
  var queryResult: QueryResult.Value<R>? = null
  var journalModeFailure: Throwable? = null

  try {
    queryResult = executeStatement(this)
  } catch(t: Throwable) {
    journalModeFailure = t
  }

  try {
    restoreForeignKeyConstraints(isForeignKeyConstraintsEnabled)
  } catch(t: Throwable) {
    val primaryFailure = journalModeFailure
    if(primaryFailure == null) {
      journalModeFailure = t
    } else if(t !== primaryFailure) {
      primaryFailure.addSuppressed(t)
    }
  }

  journalModeFailure?.let { throw it }
  return requireNotNull(queryResult)
}

private fun SQLiteConnection.restoreForeignKeyConstraints(isEnabled: Boolean) {
  // PRAGMA journal_mode currently wipes out foreign_keys - https://issuetracker.google.com/issues/447613208
  val foreignKeys = if(isEnabled) "ON" else "OFF"
  execSQL("PRAGMA foreign_keys = $foreignKeys;")
}
