package com.eygraber.sqldelight.androidx.driver

import androidx.sqlite.SQLiteConnection
import androidx.sqlite.SQLiteDriver
import androidx.sqlite.SQLiteStatement
import app.cash.sqldelight.db.QueryResult
import com.eygraber.sqldelight.androidx.driver.AndroidxSqliteConcurrencyModel.MultipleReadersSingleWriter
import kotlinx.atomicfu.atomic
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.random.Random
import kotlin.random.nextULong
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotSame
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlin.test.fail

abstract class AndroidxDriverConnectionPoolSetJournalModeTest {
  protected open val realJournalInitialMode = SqliteJournalMode.WAL
  protected open val realJournalTargetMode = SqliteJournalMode.Delete

  @Test
  fun `WAL DELETE WAL swaps reader capacity without leaking across 100 cycles`() = runBlocking {
    val factory = JournalConnectionFactory()
    val pool = createPool(factory)

    pool.assertReaderCapacity(factory, expected = 3)
    repeat(100) {
      assertEquals("delete", pool.setJournalMode(SqliteJournalMode.Delete).value)
      pool.assertReaderCapacity(factory, expected = 0)

      assertEquals("wal", pool.setJournalMode(SqliteJournalMode.WAL).value)
      pool.assertReaderCapacity(factory, expected = 3)
    }

    pool.close()
    assertEquals(factory.createdConnections.size, factory.createdConnections.count { it.isClosed })
  }

  @Test
  fun `initial WAL rejection uses non WAL reader capacity`() {
    val factory = JournalConnectionFactory(initialWriterJournalModeResult = "delete")
    val pool = createPool(factory)

    try {
      val writer = pool.acquireWriterConnection()
      pool.releaseWriterConnection()

      assertEquals(listOf("wal"), factory.writerConnection.requestedJournalModes)
      assertEquals("delete", factory.writerConnection.readJournalMode())

      val reader = pool.acquireReaderConnection()
      try {
        assertSame(writer, reader)
        assertEquals(1, factory.createdConnections.size)
      }
      finally {
        pool.releaseReaderConnection(reader)
      }
    }
    finally {
      pool.close()
    }
  }

  @Test
  fun `runtime journal change overrides the rejected initial mode`() = runBlocking {
    val factory = JournalConnectionFactory(initialWriterJournalModeResult = "delete")
    val pool = createPool(factory)

    pool.assertReaderCapacity(factory, expected = 0)
    assertEquals("wal", pool.setJournalMode(SqliteJournalMode.WAL).value)
    pool.assertReaderCapacity(factory, expected = 3)
    assertEquals(listOf("wal", "wal"), factory.writerConnection.requestedJournalModes)
    pool.close()
  }

  @Test
  fun `initial journal mode without a result row fails closed`() {
    val factory = JournalConnectionFactory(initialWriterJournalModeReturnsRow = false)
    val pool = createPool(factory)

    val failure = assertFailsWith<IllegalStateException> {
      pool.acquireWriterConnection()
    }

    assertContains(requireNotNull(failure.message), "journal_mode")
    assertContains(requireNotNull(failure.message), "no rows")
    assertEquals(1, factory.createdConnections.size)
    assertTrue(factory.createdConnections.single().isClosed)
    pool.close()
  }

  @Test
  fun `concurrent first reader waits until initial journal mode is resolved`() = runBlocking {
    val journalAssignmentEntered = CompletableDeferred<Unit>()
    val releaseJournalAssignment = CompletableDeferred<Unit>()
    val factory = JournalConnectionFactory(
      onInitialWriterJournalMode = {
        journalAssignmentEntered.complete(Unit)
        runBlocking { releaseJournalAssignment.await() }
      },
    )
    val pool = createPool(factory)
    val writerAcquire = kotlinx.coroutines.CoroutineScope(IoDispatcher).async {
      pool.acquireWriterConnection()
    }

    withTimeout(500) { journalAssignmentEntered.await() }
    val readerAcquire = kotlinx.coroutines.CoroutineScope(IoDispatcher).async {
      pool.acquireReaderConnection()
    }
    val earlyReader = withTimeoutOrNull(100) { readerAcquire.await() }
    if(earlyReader != null) {
      pool.releaseReaderConnection(earlyReader)
      releaseJournalAssignment.complete(Unit)
      writerAcquire.await()
      pool.releaseWriterConnection()
      pool.close()
      fail("A reader opened before the initial journal mode was resolved")
    }

    releaseJournalAssignment.complete(Unit)
    val writer = withTimeout(500) { writerAcquire.await() }
    val reader = withTimeout(500) { readerAcquire.await() }
    assertNotSame(writer, reader)
    pool.releaseReaderConnection(reader)
    pool.releaseWriterConnection()
    pool.close()
  }

  @Test
  fun `journal mode changes preserve the original foreign key state`() {
    listOf(false, true).forEach { initialForeignKeys ->
      val factory = JournalConnectionFactory()
      val pool = createPool(factory, isForeignKeyConstraintsEnabled = initialForeignKeys)

      pool.setJournalMode(SqliteJournalMode.Delete)
      assertEquals(initialForeignKeys, factory.writerConnection.foreignKeysEnabled)

      pool.setJournalMode(SqliteJournalMode.WAL)
      assertEquals(initialForeignKeys, factory.writerConnection.foreignKeysEnabled)

      pool.close()
    }
  }

  @Test
  fun `non String journal mapper result has a descriptive error and restores pool state`() = runBlocking {
    val factory = JournalConnectionFactory()
    val pool = createPool(factory)

    val failure = assertFailsWith<IllegalStateException> {
      pool.setJournalMode { connection ->
        executeJournalMode(connection, SqliteJournalMode.Delete)
        QueryResult.Value(42)
      }
    }

    assertContains(requireNotNull(failure.message), "journal_mode")
    assertContains(requireNotNull(failure.message), "Int")
    assertEquals("delete", factory.writerConnection.readJournalMode())
    pool.assertWriterAvailable()
    pool.assertReaderCapacity(factory, expected = 0)
    pool.close()
  }

  @Test
  fun `null journal mapper result has a descriptive error and restores pool state`() = runBlocking {
    val factory = JournalConnectionFactory()
    val pool = createPool(factory)

    val failure = assertFailsWith<IllegalStateException> {
      pool.setJournalMode { connection ->
        executeJournalMode(connection, SqliteJournalMode.Delete)
        QueryResult.Value<String?>(null)
      }
    }

    assertContains(requireNotNull(failure.message), "journal_mode")
    assertContains(requireNotNull(failure.message), "null")
    assertEquals("delete", factory.writerConnection.readJournalMode())
    pool.assertWriterAvailable()
    pool.assertReaderCapacity(factory, expected = 0)
    pool.close()
  }

  @Test
  fun `real PRAGMA readback aligns applied state after mapper failure`() = runBlocking {
    val databaseName = testDatabaseName()
    deleteDatabase(databaseName)
    val pool = AndroidxDriverConnectionPool(
      connectionFactory = androidxSqliteTestConnectionFactory(),
      nameProvider = { databaseName },
      isFileBased = true,
      configuration = AndroidxSqliteConfiguration(
        journalMode = realJournalInitialMode,
        concurrencyModel = MultipleReadersSingleWriter(
          isWal = realJournalInitialMode == SqliteJournalMode.WAL,
          walCount = 3,
          nonWalCount = 0,
        ),
      ),
    )

    try {
      val failure = assertFailsWith<IllegalStateException> {
        pool.setJournalMode { connection ->
          executeJournalMode(connection, realJournalTargetMode)
          QueryResult.Value(42)
        }
      }

      assertContains(requireNotNull(failure.message), "Int")
      androidxSqliteTestDriver().open(databaseName).use { connection ->
        assertEquals(realJournalTargetMode.value.lowercase(), connection.readJournalMode())
      }
      pool.assertReaderCapacity(expected = realJournalTargetMode.readerCountAfterSwap())
    } finally {
      runCatching { pool.close() }
      deleteDatabase(databaseName)
    }
  }

  @Test
  fun `real PRAGMA readback aligns applied state after statement close or FK restore failure`() = runBlocking {
    RealPostJournalFailure.entries.forEach { failurePoint ->
      val databaseName = testDatabaseName()
      deleteDatabase(databaseName)
      val expectedFailure = IllegalStateException("$failurePoint failed")
      val factory = RealJournalFailureConnectionFactory()
      val pool = AndroidxDriverConnectionPool(
        connectionFactory = factory,
        nameProvider = { databaseName },
        isFileBased = true,
        configuration = AndroidxSqliteConfiguration(
          journalMode = realJournalInitialMode,
          concurrencyModel = MultipleReadersSingleWriter(
            isWal = realJournalInitialMode == SqliteJournalMode.WAL,
            walCount = 3,
            nonWalCount = 0,
          ),
        ),
      )

      try {
        pool.acquireWriterConnection()
        pool.releaseWriterConnection()
        when(failurePoint) {
          RealPostJournalFailure.StatementClose ->
            factory.writerConnection.journalStatementCloseFailure = expectedFailure

          RealPostJournalFailure.ForeignKeyRestore ->
            factory.writerConnection.foreignKeyRestoreFailure = expectedFailure
        }

        val actualFailure = assertFailsWith<IllegalStateException> {
          pool.setJournalMode(realJournalTargetMode)
        }

        assertSame(expectedFailure, actualFailure)
        androidxSqliteTestDriver().open(databaseName).use { connection ->
          assertEquals(realJournalTargetMode.value.lowercase(), connection.readJournalMode())
        }
        pool.assertReaderCapacity(expected = realJournalTargetMode.readerCountAfterSwap())
      } finally {
        runCatching { pool.close() }
        deleteDatabase(databaseName)
      }
    }
  }

  @Test
  fun `statement close failure after journal change uses readback capacity`() = runBlocking {
    val factory = JournalConnectionFactory()
    val pool = createPool(factory)
    pool.acquireWriterConnection()
    pool.releaseWriterConnection()
    val closeFailure = IllegalStateException("journal statement close failed")
    factory.writerConnection.journalStatementCloseFailure = closeFailure

    val actualFailure = assertFailsWith<IllegalStateException> {
      pool.setJournalMode(SqliteJournalMode.Delete)
    }

    assertSame(closeFailure, actualFailure)
    assertEquals("delete", factory.writerConnection.readJournalMode())
    pool.assertWriterAvailable()
    pool.assertReaderCapacity(factory, expected = 0)
    pool.close()
  }

  @Test
  fun `foreign key restore failure after journal change uses readback capacity`() = runBlocking {
    val factory = JournalConnectionFactory()
    val pool = createPool(factory)
    pool.acquireWriterConnection()
    pool.releaseWriterConnection()
    val restoreFailure = IllegalStateException("foreign key restore failed")
    factory.writerConnection.foreignKeyRestoreFailure = restoreFailure

    val actualFailure = assertFailsWith<IllegalStateException> {
      pool.setJournalMode(SqliteJournalMode.Delete)
    }

    assertSame(restoreFailure, actualFailure)
    assertEquals("delete", factory.writerConnection.readJournalMode())
    pool.assertWriterAvailable()
    pool.assertReaderCapacity(factory, expected = 0)
    pool.close()
  }

  @Test
  fun `readback failure is suppressed and fails closed to zero readers`() = runBlocking {
    val factory = JournalConnectionFactory()
    val pool = createPool(factory)
    pool.acquireWriterConnection()
    pool.releaseWriterConnection()
    val readbackFailure = IllegalStateException("journal readback failed")
    factory.writerConnection.journalReadbackFailure = readbackFailure

    val actualFailure = assertFailsWith<IllegalStateException> {
      pool.setJournalMode { connection ->
        executeJournalMode(connection, SqliteJournalMode.Delete)
        QueryResult.Value<String?>(null)
      }
    }

    assertContains(requireNotNull(actualFailure.message), "null")
    assertEquals(listOf(readbackFailure), actualFailure.suppressedExceptions)
    pool.assertWriterAvailable()
    pool.assertReaderCapacity(factory, expected = 0)
    pool.close()
  }

  @Test
  fun `readback failure is primary and fails closed when journal callback succeeds`() = runBlocking {
    val factory = JournalConnectionFactory()
    val pool = createPool(factory)
    pool.acquireWriterConnection()
    pool.releaseWriterConnection()
    val readbackFailure = IllegalStateException("journal readback failed")
    factory.writerConnection.journalReadbackFailure = readbackFailure

    val actualFailure = assertFailsWith<IllegalStateException> {
      pool.setJournalMode(SqliteJournalMode.Delete)
    }

    assertSame(readbackFailure, actualFailure)
    assertTrue(actualFailure.suppressedExceptions.isEmpty())
    pool.assertWriterAvailable()
    pool.assertReaderCapacity(factory, expected = 0)
    pool.close()
  }

  @Test
  fun `configuration copy retains explicit counts and statement cache size`() {
    val configuration = AndroidxSqliteConfiguration(
      cacheSize = 25,
      journalMode = SqliteJournalMode.WAL,
      concurrencyModel = MultipleReadersSingleWriter(
        isWal = true,
        walCount = 3,
        nonWalCount = 0,
      ),
    )

    val copied = configuration.copy(journalMode = SqliteJournalMode.Delete)
    val copiedModel = assertIs<MultipleReadersSingleWriter>(copied.concurrencyModel)

    assertEquals(25, copied.cacheSize)
    assertEquals(3, copiedModel.walCount)
    assertEquals(0, copiedModel.nonWalCount)
    assertTrue(copiedModel.isWal)
    assertEquals(37, AndroidxSqliteConfiguration(cacheSize = 37).copy().cacheSize)
  }

  @Test
  fun `every journal swap failure restores writer lock and capacity matching applied state`() = runBlocking {
    JournalFailure.entries.forEach { failurePoint ->
      val factory = JournalConnectionFactory()
      val pool = createPool(factory)
      val originalReaders = List(3) { pool.acquireReaderConnection() as JournalConnection }
      originalReaders.forEach(pool::releaseReaderConnection)
      if(failurePoint != JournalFailure.ReaderClose) {
        pool.acquireWriterConnection()
        pool.releaseWriterConnection()
      }

      val expectedFailure = IllegalStateException("$failurePoint failed")
      when(failurePoint) {
        JournalFailure.ReaderClose -> originalReaders.first().closeFailure = expectedFailure
        JournalFailure.ForeignKeyRead -> factory.writerConnection.foreignKeyReadFailure = expectedFailure
        JournalFailure.JournalPragma -> factory.writerConnection.journalFailure = expectedFailure
        JournalFailure.ForeignKeyRestore -> factory.writerConnection.foreignKeyRestoreFailure = expectedFailure
      }

      val actualFailure = assertFailsWith<IllegalStateException>(message = failurePoint.name) {
        pool.setJournalMode(SqliteJournalMode.Delete)
      }

      assertSame(expectedFailure, actualFailure, failurePoint.name)
      pool.assertWriterAvailable()
      pool.assertReaderCapacity(
        factory,
        expected = if(failurePoint == JournalFailure.ForeignKeyRestore) 0 else 3,
      )
      pool.close()
      if(failurePoint == JournalFailure.ReaderClose) {
        assertTrue(originalReaders.first().isClosed, "The failed reader close must be retried by pool.close()")
      }
    }
  }

  @Test
  fun `foreign key read preserves its primary failure when statement close also fails`() = runBlocking {
    val factory = JournalConnectionFactory()
    val pool = createPool(factory)
    pool.acquireWriterConnection()
    pool.releaseWriterConnection()
    val readFailure = IllegalStateException("foreign key read failed")
    val closeFailure = IllegalStateException("foreign key statement close failed")
    factory.writerConnection.foreignKeyReadFailure = readFailure
    factory.writerConnection.foreignKeyStatementCloseFailure = closeFailure

    val actualFailure = assertFailsWith<IllegalStateException> {
      pool.setJournalMode(SqliteJournalMode.Delete)
    }

    assertSame(readFailure, actualFailure)
    assertEquals(listOf(closeFailure), actualFailure.suppressedExceptions)
    pool.assertWriterAvailable()
    pool.assertReaderCapacity(factory, expected = 3)
    pool.close()
  }

  @Test
  fun `writer configuration close failure is retained and retried before a new writer is created`() {
    val pragmaFailure = IllegalStateException("writer pragma failed")
    val firstCloseFailure = IllegalStateException("writer physical close failed")
    val factory = RetryableWriterConfigurationConnectionFactory(
      pragmaFailure = pragmaFailure,
      firstCloseFailure = firstCloseFailure,
    )
    val pool = AndroidxDriverConnectionPool(
      connectionFactory = factory,
      nameProvider = { ":memory:" },
      isFileBased = false,
      configuration = AndroidxSqliteConfiguration(),
    )

    val failure = assertFailsWith<IllegalStateException> { pool.acquireWriterConnection() }
    assertSame(pragmaFailure, failure)
    assertEquals(listOf(firstCloseFailure), failure.suppressedExceptions)
    assertEquals(1, factory.firstConnection.closeAttempts)

    val replacement = pool.acquireWriterConnection()
    pool.releaseWriterConnection()

    assertEquals(2, factory.firstConnection.closeAttempts)
    assertTrue(factory.firstConnection.isClosed)
    assertEquals(2, factory.createCount)
    assertNotSame(factory.firstConnection, replacement)
    assertEquals(
      listOf("create-1", "close-1-attempt-1", "close-1-attempt-2", "create-2"),
      factory.events,
    )
    pool.close()
  }

  @Test
  fun `pool close retries retained writer configuration connection without opening another database`() {
    val factory = RetryableWriterConfigurationConnectionFactory(
      pragmaFailure = IllegalStateException("writer pragma failed"),
      firstCloseFailure = IllegalStateException("writer physical close failed"),
    )
    val pool = AndroidxDriverConnectionPool(
      connectionFactory = factory,
      nameProvider = { ":memory:" },
      isFileBased = false,
      configuration = AndroidxSqliteConfiguration(),
    )

    assertFailsWith<IllegalStateException> { pool.acquireWriterConnection() }
    pool.close()

    assertEquals(1, factory.createCount)
    assertEquals(2, factory.firstConnection.closeAttempts)
    assertTrue(factory.firstConnection.isClosed)
    assertEquals(
      listOf("create-1", "close-1-attempt-1", "close-1-attempt-2"),
      factory.events,
    )
    pool.close()
    assertEquals(2, factory.firstConnection.closeAttempts)
  }

  @Test
  fun `passthrough journal failure closes FK snapshot and restores FK with suppressed restore failure`() {
    val factory = JournalConnectionFactory()
    val pool = createPassthroughPool(factory, isForeignKeyConstraintsEnabled = true)
    val writer = pool.acquireWriterConnection() as JournalConnection
    val journalFailure = IllegalStateException("journal callback failed")
    val restoreFailure = IllegalStateException("foreign key restore failed")
    writer.foreignKeyRestoreFailure = restoreFailure

    val actualFailure = assertFailsWith<IllegalStateException> {
      pool.setJournalMode<Unit> { connection ->
        executeJournalMode(connection, SqliteJournalMode.Delete)
        throw journalFailure
      }
    }

    assertSame(journalFailure, actualFailure)
    assertEquals(listOf(restoreFailure), actualFailure.suppressedExceptions)
    assertEquals(1, writer.foreignKeyReadStatementCloseCount)
    pool.close()
  }

  @Test
  fun `passthrough journal failure still restores original foreign key state`() {
    val factory = JournalConnectionFactory()
    val pool = createPassthroughPool(factory, isForeignKeyConstraintsEnabled = true)
    val writer = pool.acquireWriterConnection() as JournalConnection
    val journalFailure = IllegalStateException("journal callback failed")

    val actualFailure = assertFailsWith<IllegalStateException> {
      pool.setJournalMode<Unit> { connection ->
        executeJournalMode(connection, SqliteJournalMode.Delete)
        throw journalFailure
      }
    }

    assertSame(journalFailure, actualFailure)
    assertTrue(writer.foreignKeysEnabled)
    assertEquals(1, writer.foreignKeyReadStatementCloseCount)
    pool.close()
  }

  @Test
  fun `closing uninitialized passthrough pool does not open the database`() {
    val factory = JournalConnectionFactory()
    val pool = createPassthroughPool(factory)

    pool.close()

    assertTrue(factory.createdConnections.isEmpty())
  }

  @Test
  fun `journal swap blocks new readers and waits for checked out reader and writer`() = runBlocking {
    val factory = JournalConnectionFactory()
    val readerWaitingAtJournalGate = CompletableDeferred<Unit>()
    val pool = createPool(
      factory = factory,
      onReaderJournalModeGateWait = { readerWaitingAtJournalGate.complete(Unit) },
    )
    val readers = List(3) { pool.acquireReaderConnection() }
    readers.drop(1).forEach(pool::releaseReaderConnection)
    pool.acquireWriterConnection()
    val journalExecuted = CompletableDeferred<Unit>()

    val swap = kotlinx.coroutines.CoroutineScope(IoDispatcher).async {
      pool.setJournalMode { connection ->
        journalExecuted.complete(Unit)
        executeJournalMode(connection, SqliteJournalMode.WAL)
      }
    }

    withTimeout(500) { factory.twoReadersClosed.await() }
    val newReader = kotlinx.coroutines.CoroutineScope(IoDispatcher).async {
      pool.acquireReaderConnection()
    }
    val reachedJournalGate = withTimeoutOrNull(500) {
      readerWaitingAtJournalGate.await()
      true
    } ?: false
    if(!reachedJournalGate) {
      pool.releaseReaderConnection(readers.first())
      pool.releaseWriterConnection()
      swap.await()
      pool.releaseReaderConnection(newReader.await())
      pool.close()
      fail("The reader never reported waiting on the journal-mode gate")
    }
    assertFalse(newReader.isCompleted, "A new reader acquired while the journal swap held its gate")
    assertFalse(journalExecuted.isCompleted, "Journal PRAGMA ran before the checked out reader returned")

    pool.releaseReaderConnection(readers.first())
    withTimeout(500) { factory.allReadersClosed.await() }
    assertFalse(journalExecuted.isCompleted, "Journal PRAGMA ran before the active writer was released")

    pool.releaseWriterConnection()
    withTimeout(500) { journalExecuted.await() }
    withTimeout(500) { swap.await() }
    val acquiredAfterSwap = withTimeout(500) { newReader.await() }
    pool.releaseReaderConnection(acquiredAfterSwap)
    pool.close()
  }

  private fun createPool(
    factory: JournalConnectionFactory,
    isForeignKeyConstraintsEnabled: Boolean = true,
    onReaderJournalModeGateWait: () -> Unit = {},
  ) = AndroidxDriverConnectionPool(
    connectionFactory = factory,
    nameProvider = { "journal-test.db" },
    isFileBased = true,
    configuration = AndroidxSqliteConfiguration(
      isForeignKeyConstraintsEnabled = isForeignKeyConstraintsEnabled,
      concurrencyModel = MultipleReadersSingleWriter(
        isWal = true,
        walCount = 3,
        nonWalCount = 0,
      ),
    ),
    onReaderJournalModeGateWait = onReaderJournalModeGateWait,
  )

  private fun createPassthroughPool(
    factory: JournalConnectionFactory,
    isForeignKeyConstraintsEnabled: Boolean = false,
  ) = PassthroughConnectionPool(
    connectionFactory = factory,
    nameProvider = { "passthrough-journal-test.db" },
    configuration = AndroidxSqliteConfiguration(
      isForeignKeyConstraintsEnabled = isForeignKeyConstraintsEnabled,
    ),
  )

  private fun testDatabaseName() =
    "${this::class.qualifiedName}.${Random.nextULong().toHexString()}.db"

  private fun deleteDatabase(databaseName: String) {
    deleteFile(databaseName)
    deleteFile("$databaseName-shm")
    deleteFile("$databaseName-wal")
  }
}

private enum class JournalFailure {
  ReaderClose,
  ForeignKeyRead,
  JournalPragma,
  ForeignKeyRestore,
}

private enum class RealPostJournalFailure {
  StatementClose,
  ForeignKeyRestore,
}

private fun SqliteJournalMode.readerCountAfterSwap() = if(this == SqliteJournalMode.WAL) 3 else 0

private fun ConnectionPool.setJournalMode(mode: SqliteJournalMode): QueryResult.Value<String> =
  setJournalMode { connection -> executeJournalMode(connection, mode) }

private fun executeJournalMode(
  connection: SQLiteConnection,
  mode: SqliteJournalMode,
): QueryResult.Value<String> =
  connection.prepare("PRAGMA journal_mode = ${mode.value};").let { statement ->
    try {
      statement.step()
      QueryResult.Value(requireNotNull(statement.getText(0)))
    } finally {
      statement.close()
    }
  }

private fun SQLiteConnection.readJournalMode(): String =
  prepare("PRAGMA journal_mode;").use { statement ->
    statement.step()
    requireNotNull(statement.getText(0))
  }

private suspend fun ConnectionPool.assertWriterAvailable() {
  val writerAcquire = kotlinx.coroutines.CoroutineScope(IoDispatcher).async {
    acquireWriterConnection()
  }
  val writer = withTimeoutOrNull(500) { writerAcquire.await() }
  if(writer == null) {
    releaseWriterConnection()
    writerAcquire.await()
    fail("The journal swap failure retained the writer lock")
  }
  releaseWriterConnection()
}

private suspend fun ConnectionPool.assertReaderCapacity(
  factory: JournalConnectionFactory,
  expected: Int,
) {
  if(expected == 0) {
    val reader = acquireReaderConnection()
    releaseReaderConnection(reader)
    val writer = acquireWriterConnection()
    releaseWriterConnection()
    assertSame(writer, reader, "A zero-capacity reader pool must route reads through the writer")
    return
  }

  val readerAcquires = List(expected) {
    kotlinx.coroutines.CoroutineScope(IoDispatcher).async { acquireReaderConnection() }
  }
  val readers = withTimeoutOrNull(500) { readerAcquires.awaitAll() }
  if(readers == null) {
    repeat(expected) { releaseReaderConnection(factory.createRescueConnection()) }
    readerAcquires.awaitAll()
    fail("Expected reader capacity $expected was not restored")
  }

  assertEquals(expected, readers.toSet().size)
  val extraAcquire = kotlinx.coroutines.CoroutineScope(IoDispatcher).async { acquireReaderConnection() }
  val unexpectedExtra = withTimeoutOrNull(25) { extraAcquire.await() }
  assertEquals(null, unexpectedExtra, "Reader capacity exceeded $expected")

  releaseReaderConnection(readers.first())
  val unblockedExtra = extraAcquire.await()
  readers.drop(1).forEach(::releaseReaderConnection)
  releaseReaderConnection(unblockedExtra)
}

private suspend fun ConnectionPool.assertReaderCapacity(expected: Int) {
  if(expected == 0) {
    val reader = acquireReaderConnection()
    releaseReaderConnection(reader)
    val writer = acquireWriterConnection()
    releaseWriterConnection()
    assertSame(writer, reader, "A zero-capacity reader pool must route reads through the writer")
    return
  }

  val readerAcquires = List(expected) {
    kotlinx.coroutines.CoroutineScope(IoDispatcher).async { acquireReaderConnection() }
  }
  val readers = withTimeoutOrNull(500) { readerAcquires.awaitAll() }
  if(readers == null) {
    readerAcquires.forEach { it.cancel() }
    fail("Expected reader capacity $expected was not restored")
  }

  assertEquals(expected, readers.toSet().size)
  readers.forEach(::releaseReaderConnection)
}

private class JournalConnectionFactory(
  private val initialWriterJournalModeResult: String? = null,
  private val initialWriterJournalModeReturnsRow: Boolean = true,
  private val onInitialWriterJournalMode: () -> Unit = {},
) : AndroidxSqliteConnectionFactory {
  override val driver = object : SQLiteDriver {
    override fun open(fileName: String): SQLiteConnection = JournalConnection()
  }

  val createdConnections = mutableListOf<JournalConnection>()
  private val closedReaderCount = atomic(0)
  val twoReadersClosed = CompletableDeferred<Unit>()
  val allReadersClosed = CompletableDeferred<Unit>()
  val writerConnection: JournalConnection
    get() = createdConnections.first { it.isWriter }

  override fun createConnection(name: String): SQLiteConnection {
    val isInitialWriter = createdConnections.isEmpty()
    val initialJournalModeCallback: () -> Unit =
      if(isInitialWriter) onInitialWriterJournalMode else ({})
    return JournalConnection(
      initialJournalModeResult = initialWriterJournalModeResult.takeIf { isInitialWriter },
      initialJournalModeReturnsRow = if(isInitialWriter) initialWriterJournalModeReturnsRow else true,
      onInitialJournalMode = initialJournalModeCallback,
      onClosed = ::onConnectionClosed,
    ).also(createdConnections::add)
  }

  fun createRescueConnection() = JournalConnection(onClosed = ::onConnectionClosed).also(createdConnections::add)

  private fun onConnectionClosed(connection: JournalConnection) {
    if(connection.isWriter) return
    when(closedReaderCount.incrementAndGet()) {
      2 -> twoReadersClosed.complete(Unit)
      3 -> allReadersClosed.complete(Unit)
    }
  }
}

private class JournalConnection(
  val initialJournalModeResult: String? = null,
  val initialJournalModeReturnsRow: Boolean = true,
  val onInitialJournalMode: () -> Unit = {},
  private val onClosed: (JournalConnection) -> Unit = {},
) : SQLiteConnection {
  var closeFailure: Throwable? = null
  var foreignKeyReadFailure: Throwable? = null
  var foreignKeyStatementCloseFailure: Throwable? = null
  var foreignKeyReadStatementCloseCount = 0
  var journalFailure: Throwable? = null
  var journalReadbackFailure: Throwable? = null
  var journalStatementCloseFailure: Throwable? = null
  var foreignKeyRestoreFailure: Throwable? = null
  var foreignKeysEnabled = false
  var journalMode = "delete"
  var isClosed = false
  var isWriter = false
  val requestedJournalModes = mutableListOf<String>()
  var isInitialJournalModeConsumed = false

  override fun prepare(sql: String): SQLiteStatement {
    if(sql.startsWith("PRAGMA synchronous")) isWriter = true
    return JournalStatement(this, sql)
  }

  override fun close() {
    closeFailure?.let { failure ->
      closeFailure = null
      throw failure
    }
    if(!isClosed) {
      isClosed = true
      onClosed(this)
    }
  }
}

private class JournalStatement(
  private val connection: JournalConnection,
  private val sql: String,
) : SQLiteStatement {
  private var textResult: String? = null

  override fun step(): Boolean {
    when {
      sql == "PRAGMA foreign_keys;" -> connection.foreignKeyReadFailure?.let { failure ->
        connection.foreignKeyReadFailure = null
        throw failure
      }

      sql.startsWith("PRAGMA foreign_keys = ") -> {
        connection.foreignKeyRestoreFailure?.let { failure ->
          connection.foreignKeyRestoreFailure = null
          throw failure
        }
        connection.foreignKeysEnabled = sql.contains("ON", ignoreCase = true)
      }

      sql.startsWith("PRAGMA journal_mode = ") -> {
        connection.journalFailure?.let { failure ->
          connection.journalFailure = null
          throw failure
        }
        val requestedMode = sql.substringAfter("=").substringBefore(";").trim().lowercase()
        connection.requestedJournalModes += requestedMode
        val isInitialAssignment = !connection.isInitialJournalModeConsumed
        if(isInitialAssignment) {
          connection.isInitialJournalModeConsumed = true
          connection.onInitialJournalMode()
        }
        connection.journalMode = if(isInitialAssignment) {
          connection.initialJournalModeResult ?: requestedMode
        }
        else {
          requestedMode
        }
        textResult = connection.journalMode
        connection.foreignKeysEnabled = false
        if(isInitialAssignment && !connection.initialJournalModeReturnsRow) return false
      }

      sql == "PRAGMA journal_mode;" -> {
        connection.journalReadbackFailure?.let { failure ->
          connection.journalReadbackFailure = null
          throw failure
        }
        textResult = connection.journalMode
      }
    }
    return true
  }

  override fun getBoolean(index: Int) = connection.foreignKeysEnabled
  override fun getText(index: Int) = requireNotNull(textResult)
  override fun getLong(index: Int) = if(connection.foreignKeysEnabled) 1L else 0L
  override fun getDouble(index: Int) = getLong(index).toDouble()
  override fun getBlob(index: Int) = ByteArray(0)
  override fun isNull(index: Int) = false
  override fun getColumnCount() = 1
  override fun getColumnName(index: Int) = "journal_mode"
  override fun getColumnType(index: Int) = 3
  override fun bindBlob(index: Int, value: ByteArray) {}
  override fun bindDouble(index: Int, value: Double) {}
  override fun bindLong(index: Int, value: Long) {}
  override fun bindText(index: Int, value: String) {}
  override fun bindNull(index: Int) {}
  override fun clearBindings() {}
  override fun close() {
    if(sql == "PRAGMA foreign_keys;") {
      connection.foreignKeyReadStatementCloseCount++
      connection.foreignKeyStatementCloseFailure?.let { failure ->
        connection.foreignKeyStatementCloseFailure = null
        throw failure
      }
    }
    if(sql.startsWith("PRAGMA journal_mode = ")) {
      connection.journalStatementCloseFailure?.let { failure ->
        connection.journalStatementCloseFailure = null
        throw failure
      }
    }
  }
  override fun reset() {}
}

private class RetryableWriterConfigurationConnectionFactory(
  private val pragmaFailure: Throwable,
  firstCloseFailure: Throwable,
) : AndroidxSqliteConnectionFactory {
  override val driver = androidxSqliteTestDriver()
  val events = mutableListOf<String>()
  val firstConnection = RetryableWriterConfigurationConnection(
    pragmaFailure = pragmaFailure,
    firstCloseFailure = firstCloseFailure,
    onCloseAttempt = { attempt -> events += "close-1-attempt-$attempt" },
  )

  var createCount = 0
    private set

  override fun createConnection(name: String): SQLiteConnection {
    createCount++
    events += "create-$createCount"
    return if(createCount == 1) firstConnection else driver.open(name)
  }
}

private class RetryableWriterConfigurationConnection(
  private val pragmaFailure: Throwable,
  private val firstCloseFailure: Throwable,
  private val onCloseAttempt: (Int) -> Unit,
) : SQLiteConnection {
  var closeAttempts = 0
    private set
  var isClosed = false
    private set

  override fun prepare(sql: String): SQLiteStatement = throw pragmaFailure

  override fun close() {
    closeAttempts++
    onCloseAttempt(closeAttempts)
    if(closeAttempts == 1) throw firstCloseFailure
    isClosed = true
  }
}

private class RealJournalFailureConnectionFactory : AndroidxSqliteConnectionFactory {
  override val driver = androidxSqliteTestDriver()
  val createdConnections = mutableListOf<RealJournalFailureConnection>()
  val writerConnection: RealJournalFailureConnection
    get() = createdConnections.first { it.isWriter }

  override fun createConnection(name: String): SQLiteConnection =
    RealJournalFailureConnection(driver.open(name)).also(createdConnections::add)
}

private class RealJournalFailureConnection(
  private val delegate: SQLiteConnection,
) : SQLiteConnection by delegate {
  var isWriter = false
    private set
  var journalStatementCloseFailure: Throwable? = null
  var foreignKeyRestoreFailure: Throwable? = null

  override fun prepare(sql: String): SQLiteStatement {
    if(sql.startsWith("PRAGMA synchronous")) isWriter = true
    val statement = delegate.prepare(sql)
    return when {
      sql.startsWith("PRAGMA journal_mode = ") -> object : SQLiteStatement by statement {
        override fun close() {
          statement.close()
          journalStatementCloseFailure?.let { failure ->
            journalStatementCloseFailure = null
            throw failure
          }
        }
      }

      sql.startsWith("PRAGMA foreign_keys = ") -> object : SQLiteStatement by statement {
        override fun step(): Boolean {
          foreignKeyRestoreFailure?.let { failure ->
            foreignKeyRestoreFailure = null
            throw failure
          }
          return statement.step()
        }
      }

      else -> statement
    }
  }
}
