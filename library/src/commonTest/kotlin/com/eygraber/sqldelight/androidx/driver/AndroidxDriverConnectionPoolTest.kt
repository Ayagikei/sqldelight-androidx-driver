package com.eygraber.sqldelight.androidx.driver

import androidx.sqlite.SQLiteConnection
import androidx.sqlite.SQLiteDriver
import androidx.sqlite.SQLiteStatement
import app.cash.sqldelight.db.AfterVersion
import app.cash.sqldelight.db.QueryResult
import app.cash.sqldelight.db.SqlDriver
import app.cash.sqldelight.db.SqlSchema
import com.eygraber.sqldelight.androidx.driver.AndroidxSqliteConcurrencyModel.MultipleReadersSingleWriter
import com.eygraber.sqldelight.androidx.driver.AndroidxSqliteConcurrencyModel.SingleReaderWriter
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.random.Random
import kotlin.random.nextULong
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

private const val TRACKED_QUERY = "SELECT 42"

abstract class AndroidxDriverConnectionPoolTest {
  private val temporaryDatabaseNames = mutableListOf<String>()

  @AfterTest
  fun cleanTemporaryDatabases() {
    temporaryDatabaseNames.forEach(::deleteDatabase)
    temporaryDatabaseNames.clear()
  }

  @Test
  fun `writer creation failure releases mutex for the next acquire`() = runBlocking {
    val pool = AndroidxDriverConnectionPool(
      connectionFactory = FailingConnectionFactory(failingAttempts = setOf(1)),
      nameProvider = { ":memory:" },
      isFileBased = false,
      configuration = AndroidxSqliteConfiguration(concurrencyModel = SingleReaderWriter),
    )

    assertFailsWith<IllegalStateException> {
      pool.acquireWriterConnection()
    }

    val nextAcquire = async(IoDispatcher) {
      pool.acquireWriterConnection()
    }
    val acquiredWithoutTimeout = withTimeoutOrNull(500) {
      nextAcquire.await()
      true
    } ?: false

    if(!acquiredWithoutTimeout) {
      // Unblock the legacy implementation so the RED test can terminate cleanly.
      pool.releaseWriterConnection()
      nextAcquire.await()
    }
    pool.releaseWriterConnection()
    pool.close()

    assertTrue(
      actual = acquiredWithoutTimeout,
      message = "The second writer acquire timed out because the failed initializer retained the mutex",
    )
  }

  @Test
  fun `reader creation failure restores the full pool capacity`() = runBlocking {
    val databaseName = "${this::class.qualifiedName}.${Random.nextULong().toHexString()}.db"
    deleteDatabase(databaseName)
    try {
      val pool = AndroidxDriverConnectionPool(
        connectionFactory = FailingConnectionFactory(failingAttempts = setOf(2)),
        nameProvider = { databaseName },
        isFileBased = true,
        configuration = AndroidxSqliteConfiguration(
          concurrencyModel = MultipleReadersSingleWriter(isWal = true, walCount = 3),
        ),
      )

      assertFailsWith<IllegalStateException> {
        pool.acquireReaderConnection()
      }

      val firstReader = pool.acquireReaderConnection()
      val secondReader = pool.acquireReaderConnection()
      val thirdAcquire = async(IoDispatcher) {
        pool.acquireReaderConnection()
      }
      var thirdReader: SQLiteConnection? = null
      val acquiredWithoutTimeout = withTimeoutOrNull(500) {
        thirdReader = thirdAcquire.await()
        true
      } ?: false

      if(acquiredWithoutTimeout) {
        pool.releaseReaderConnection(firstReader)
        pool.releaseReaderConnection(secondReader)
        pool.releaseReaderConnection(requireNotNull(thirdReader))
        pool.close()
      } else {
        // Unblock the legacy implementation without asking its already-shrunken pool to drain 3 slots.
        pool.releaseReaderConnection(firstReader)
        thirdReader = thirdAcquire.await()
        firstReader.close()
        secondReader.close()
      }

      assertTrue(
        actual = acquiredWithoutTimeout,
        message = "The failed reader initializer permanently reduced the pool capacity below 3",
      )
    } finally {
      deleteDatabase(databaseName)
    }
  }

  @Test
  fun `writer configuration failure retains failed close for pool close retry`() {
    val pragmaFailure = IllegalStateException("writer pragma failed")
    val closeFailure = IllegalStateException("opened connection close failed")
    val connectionFactory = WriterConfigurationFailingConnectionFactory(
      pragmaFailure = pragmaFailure,
      closeFailure = closeFailure,
    )
    val pool = AndroidxDriverConnectionPool(
      connectionFactory = connectionFactory,
      nameProvider = { ":memory:" },
      isFileBased = false,
      configuration = AndroidxSqliteConfiguration(concurrencyModel = SingleReaderWriter),
    )

    val failure = assertFailsWith<IllegalStateException> {
      pool.acquireWriterConnection()
    }

    assertSame(pragmaFailure, failure)
    assertEquals(listOf(closeFailure), failure.suppressedExceptions)
    assertEquals(1, connectionFactory.closeAttempts)
    pool.close()
    assertEquals(2, connectionFactory.closeAttempts)
  }

  @Test
  fun `closing a reader evicts and closes its cached statements`() {
    val databaseName = "${this::class.qualifiedName}.${Random.nextULong().toHexString()}.db"
    deleteDatabase(databaseName)
    val connectionFactory = TrackingConnectionFactory()
    val driver = AndroidxSqliteDriver(
      connectionFactory = connectionFactory,
      databaseType = AndroidxSqliteDatabaseType.File(databaseName),
      schema = EmptySchema,
      configuration = AndroidxSqliteConfiguration(
        cacheSize = 4,
        concurrencyModel = MultipleReadersSingleWriter(isWal = true, walCount = 1),
      ),
    )

    try {
      driver.executeQuery(
        identifier = 42,
        sql = TRACKED_QUERY,
        mapper = { cursor ->
          cursor.next()
          QueryResult.Unit
        },
        parameters = 0,
      )
      val cachedStatement = connectionFactory.statements.single { it.sql == TRACKED_QUERY }
      assertEquals(0, cachedStatement.closeCount)

      driver.setJournalMode(SqliteJournalMode.WAL)

      assertEquals(
        expected = 1,
        actual = cachedStatement.closeCount,
        message = "Closing the reader must evict and close its cached statement",
      )
    } finally {
      driver.close()
      deleteDatabase(databaseName)
    }
  }

  @Test
  fun `failed reader statement cleanup is retained until a later close succeeds`() {
    val databaseName = "${this::class.qualifiedName}.${Random.nextULong().toHexString()}.db"
    deleteDatabase(databaseName)
    val connectionFactory = TrackingConnectionFactory(trackedQueryCloseFailures = 2)
    val driver = AndroidxSqliteDriver(
      connectionFactory = connectionFactory,
      databaseType = AndroidxSqliteDatabaseType.File(databaseName),
      schema = EmptySchema,
      configuration = AndroidxSqliteConfiguration(
        cacheSize = 4,
        concurrencyModel = MultipleReadersSingleWriter(isWal = true, walCount = 1),
      ),
    )

    try {
      driver.executeQuery(
        identifier = 43,
        sql = TRACKED_QUERY,
        mapper = { cursor ->
          cursor.next()
          QueryResult.Unit
        },
        parameters = 0,
      )
      val cachedStatement = connectionFactory.statements.single { it.sql == TRACKED_QUERY }

      val swapFailure = assertFailsWith<IllegalStateException> {
        driver.setJournalMode(SqliteJournalMode.WAL)
      }
      assertContains(requireNotNull(swapFailure.message), "attempt 1")
      assertEquals(1, cachedStatement.closeCount)

      val retryFailure = assertFailsWith<IllegalStateException> { driver.close() }
      assertContains(requireNotNull(retryFailure.message), "attempt 2")
      assertEquals(2, cachedStatement.closeCount)

      driver.close()
      assertEquals(3, cachedStatement.closeCount)

      driver.close()
      assertEquals(3, cachedStatement.closeCount)
    } finally {
      runCatching { driver.close() }
      deleteDatabase(databaseName)
    }
  }

  @Test
  fun `close with an outstanding reader is non destructive and can be retried`() = runBlocking {
    val databaseName = temporaryDatabaseName()
    val pool = AndroidxDriverConnectionPool(
      connectionFactory = FailingConnectionFactory(failingAttempts = emptySet()),
      nameProvider = { databaseName },
      isFileBased = true,
      configuration = AndroidxSqliteConfiguration(
        concurrencyModel = MultipleReadersSingleWriter(isWal = true, walCount = 3),
      ),
    )
    val checkedOutReader = pool.acquireReaderConnection()

    val closeAttempt = async(IoDispatcher) {
      runCatching { pool.close() }
    }
    val completedClose = withTimeoutOrNull(500) {
      closeAttempt.await()
    }

    if(completedClose == null) {
      // Unblock the legacy implementation so the RED test can terminate cleanly.
      pool.releaseReaderConnection(checkedOutReader)
      closeAttempt.await()
    }

    val failure = assertNotNull(
      actual = completedClose,
      message = "close() hung while waiting for a checked-out reader",
    ).exceptionOrNull()
    assertIs<IllegalStateException>(failure)
    assertContains(
      charSequence = requireNotNull(failure.message),
      other = "1 reader connection(s) still checked out",
    )

    val retryClose = runCatching {
      pool.releaseReaderConnection(checkedOutReader)
      pool.close()
    }
    if(retryClose.isFailure) checkedOutReader.close()
    assertTrue(
      actual = retryClose.isSuccess,
      message = "The failed close destroyed drained reader entries or closed the pool channel",
    )
  }

  @Test
  fun `writer close failure retains drained readers and retries only the writer`() = runBlocking {
    val databaseName = temporaryDatabaseName()
    val connectionFactory = PoolCloseTrackingConnectionFactory()
    val closeCallback = RetryablePoolCloseCallback()
    val pool = AndroidxDriverConnectionPool(
      connectionFactory = connectionFactory,
      nameProvider = { databaseName },
      isFileBased = true,
      configuration = AndroidxSqliteConfiguration(
        concurrencyModel = MultipleReadersSingleWriter(isWal = true, walCount = 2),
      ),
      closeConnection = closeCallback::close,
    )
    val writer = pool.acquireWriterConnection()
    pool.releaseWriterConnection()
    val readers = List(2) { pool.acquireReaderConnection() }
    readers.forEach(pool::releaseReaderConnection)
    val writerFailure = IllegalStateException("writer close failed")
    closeCallback.failNext(writer, writerFailure)

    val firstFailure = assertFailsWith<IllegalStateException> { pool.close() }

    assertSame(writerFailure, firstFailure)
    assertFalse(closeCallback.wasClosed(writer))
    readers.forEach { reader -> assertTrue(closeCallback.wasClosed(reader)) }

    pool.close()

    assertEquals(2, closeCallback.attempts(writer))
    readers.forEach { reader -> assertEquals(1, closeCallback.attempts(reader)) }
    assertTrue(closeCallback.wasClosed(writer))

    pool.close()
    assertEquals(2, closeCallback.attempts(writer))
    readers.forEach { reader -> assertEquals(1, closeCallback.attempts(reader)) }
  }

  @Test
  fun `reader close failures are aggregated and retained until retry succeeds`() = runBlocking {
    val databaseName = temporaryDatabaseName()
    val connectionFactory = PoolCloseTrackingConnectionFactory()
    val closeCallback = RetryablePoolCloseCallback()
    val pool = AndroidxDriverConnectionPool(
      connectionFactory = connectionFactory,
      nameProvider = { databaseName },
      isFileBased = true,
      configuration = AndroidxSqliteConfiguration(
        concurrencyModel = MultipleReadersSingleWriter(isWal = true, walCount = 2),
      ),
      closeConnection = closeCallback::close,
    )
    val writer = pool.acquireWriterConnection()
    pool.releaseWriterConnection()
    val readers = List(2) { pool.acquireReaderConnection() }
    readers.forEach(pool::releaseReaderConnection)
    val firstReaderFailure = IllegalStateException("first reader close failed")
    val secondReaderFailure = IllegalStateException("second reader close failed")
    closeCallback.failNext(readers[0], firstReaderFailure)
    closeCallback.failNext(readers[1], secondReaderFailure)

    val firstFailure = assertFailsWith<IllegalStateException> { pool.close() }

    assertSame(firstReaderFailure, firstFailure)
    assertEquals(listOf(secondReaderFailure), firstFailure.suppressedExceptions)
    assertTrue(closeCallback.wasClosed(writer))
    readers.forEach { reader -> assertFalse(closeCallback.wasClosed(reader)) }

    pool.close()

    assertEquals(1, closeCallback.attempts(writer))
    readers.forEach { reader ->
      assertEquals(2, closeCallback.attempts(reader))
      assertTrue(closeCallback.wasClosed(reader))
    }
  }

  private fun deleteDatabase(databaseName: String) {
    deleteFile(databaseName)
    deleteFile("$databaseName-shm")
    deleteFile("$databaseName-wal")
  }

  private fun temporaryDatabaseName(): String =
    "${this::class.qualifiedName}.${Random.nextULong().toHexString()}.db".also { databaseName ->
      deleteDatabase(databaseName)
      temporaryDatabaseNames += databaseName
    }

  private companion object {
    val EmptySchema = object : SqlSchema<QueryResult.Value<Unit>> {
      override val version = 1L

      override fun create(driver: SqlDriver) = QueryResult.Unit

      override fun migrate(
        driver: SqlDriver,
        oldVersion: Long,
        newVersion: Long,
        vararg callbacks: AfterVersion,
      ) = QueryResult.Unit
    }
  }
}

private class FailingConnectionFactory(
  private val failingAttempts: Set<Int>,
) : AndroidxSqliteConnectionFactory {
  override val driver: SQLiteDriver = androidxSqliteTestDriver()

  private var attempt = 0

  override fun createConnection(name: String): SQLiteConnection {
    attempt++
    if(attempt in failingAttempts) {
      throw IllegalStateException("connection creation failed on attempt $attempt")
    }
    return driver.open(name)
  }
}

private class WriterConfigurationFailingConnectionFactory(
  private val pragmaFailure: Throwable,
  private val closeFailure: Throwable,
) : AndroidxSqliteConnectionFactory {
  override val driver: SQLiteDriver = androidxSqliteTestDriver()

  var closeAttempts = 0
    private set

  override fun createConnection(name: String): SQLiteConnection = object : SQLiteConnection {
    override fun prepare(sql: String): SQLiteStatement = throw pragmaFailure

    override fun close() {
      closeAttempts++
      if(closeAttempts == 1) throw closeFailure
    }
  }
}

private class TrackingConnectionFactory(
  private val trackedQueryCloseFailures: Int = 0,
) : AndroidxSqliteConnectionFactory {
  override val driver: SQLiteDriver = externalPoolTestDriver()
  val statements = mutableListOf<TrackingStatement>()

  override fun createConnection(name: String): SQLiteConnection = object : SQLiteConnection {
    private val delegate = driver.open(name)

    override fun prepare(sql: String): SQLiteStatement =
      TrackingStatement(
        sql = sql,
        delegate = delegate.prepare(sql),
        closeFailures = if(sql == TRACKED_QUERY) trackedQueryCloseFailures else 0,
      ).also(statements::add)

    override fun close() {
      delegate.close()
    }
  }
}

private fun externalPoolTestDriver(): SQLiteDriver {
  val delegate = androidxSqliteTestDriver()
  return object : SQLiteDriver by delegate {
    override val hasConnectionPool = false
  }
}

private class TrackingStatement(
  val sql: String,
  private val delegate: SQLiteStatement,
  private val closeFailures: Int,
) : SQLiteStatement by delegate {
  var closeCount = 0
    private set

  override fun close() {
    closeCount++
    if(closeCount <= closeFailures) {
      throw IllegalStateException("tracked statement close failed on attempt $closeCount")
    }
    delegate.close()
  }
}

private class PoolCloseTrackingConnectionFactory : AndroidxSqliteConnectionFactory {
  override val driver: SQLiteDriver = androidxSqliteTestDriver()

  override fun createConnection(name: String): SQLiteConnection = driver.open(name)
}

private class RetryablePoolCloseCallback {
  private val closeAttempts = mutableMapOf<SQLiteConnection, Int>()
  private val failures = mutableMapOf<SQLiteConnection, Throwable>()
  private val closedConnections = mutableSetOf<SQLiteConnection>()

  fun failNext(connection: SQLiteConnection, failure: Throwable) {
    failures[connection] = failure
  }

  fun close(connection: SQLiteConnection) {
    closeAttempts[connection] = attempts(connection) + 1
    failures.remove(connection)?.let { throw it }
    connection.close()
    closedConnections += connection
  }

  fun attempts(connection: SQLiteConnection) = closeAttempts[connection] ?: 0

  fun wasClosed(connection: SQLiteConnection) = connection in closedConnections
}
