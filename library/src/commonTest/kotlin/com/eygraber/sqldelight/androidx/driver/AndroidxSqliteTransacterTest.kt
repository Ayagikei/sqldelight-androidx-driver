package com.eygraber.sqldelight.androidx.driver

import androidx.sqlite.SQLiteConnection
import androidx.sqlite.SQLiteStatement
import app.cash.sqldelight.TransacterImpl
import app.cash.sqldelight.db.AfterVersion
import app.cash.sqldelight.db.QueryResult
import app.cash.sqldelight.db.SqlDriver
import app.cash.sqldelight.db.SqlSchema
import kotlinx.atomicfu.atomic
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

abstract class AndroidxSqliteTransacterTest {
  private lateinit var transacter: TransacterImpl
  private lateinit var driver: SqlDriver

  @Suppress("VisibleForTests")
  private fun setupDatabase(
    schema: SqlSchema<QueryResult.Value<Unit>>,
    connectionPool: ConnectionPool? = null,
  ): SqlDriver = AndroidxSqliteDriver(
    connectionFactory = androidxSqliteTestConnectionFactory(),
    databaseType = AndroidxSqliteDatabaseType.Memory,
    schema = schema,
    overridingConnectionPool = connectionPool,
  )

  @BeforeTest
  fun setup() {
    val driver = setupDatabase(
      object : SqlSchema<QueryResult.Value<Unit>> {
        override val version = 1L
        override fun create(driver: SqlDriver): QueryResult.Value<Unit> = QueryResult.Unit
        override fun migrate(
          driver: SqlDriver,
          oldVersion: Long,
          newVersion: Long,
          vararg callbacks: AfterVersion,
        ): QueryResult.Value<Unit> = QueryResult.Unit
      },
    )
    transacter = object : TransacterImpl(driver) {}
    this.driver = driver
  }

  @AfterTest
  fun teardown() {
    driver.close()
  }

  @Test
  fun ifBeginningANonEnclosedTransactionFails_furtherTransactionsAreNotBlockedFromBeginning() = runBlocking {
    this@AndroidxSqliteTransacterTest.driver.close()

    val connectionPool = FirstUserTransactionFailsConnectionPool()
    val driver = setupDatabase(
      object : SqlSchema<QueryResult.Value<Unit>> {
        override val version = 1L
        override fun create(driver: SqlDriver): QueryResult.Value<Unit> = QueryResult.Unit
        override fun migrate(
          driver: SqlDriver,
          oldVersion: Long,
          newVersion: Long,
          vararg callbacks: AfterVersion,
        ): QueryResult.Value<Unit> = QueryResult.Unit
      },
      connectionPool = connectionPool,
    )
    val transacter = object : TransacterImpl(driver) {}
    this@AndroidxSqliteTransacterTest.driver = driver
    assertFails {
      transacter.transaction(noEnclosing = true) {}
    }
    assertNull(driver.currentTransaction())

    val retry = async(IoDispatcher) {
      transacter.transaction(noEnclosing = true) {}
    }
    val retryCompleted = withTimeoutOrNull(500) {
      retry.await()
      true
    } ?: false
    if(!retryCompleted) {
      connectionPool.recoverLeakedWriterForTestCleanup()
      retry.await()
    }

    assertTrue(retryCompleted, "The failed BEGIN retained writer ownership and blocked the next transaction")
  }

  @Test
  fun `raw journal mode execute fails fast inside transaction`() {
    val connectionPool = installDriver(JournalModeTransactionGuardConnectionPool())

    transacter.transaction {
      val failure = assertFailsWith<IllegalStateException> {
        driver.execute(null, "PRAGMA journal_mode = WAL;", 0).value
      }
      assertContains(requireNotNull(failure.message), "cannot be called from within a transaction")
    }

    assertEquals(0, connectionPool.setJournalModeCalls)
    transacter.transaction {}
  }

  @Test
  fun `raw journal mode query fails fast inside transaction`() {
    val connectionPool = installDriver(JournalModeTransactionGuardConnectionPool())

    transacter.transaction {
      val failure = assertFailsWith<IllegalStateException> {
        driver.executeQuery(
          identifier = null,
          sql = "PRAGMA journal_mode = WAL;",
          mapper = { QueryResult.Value(Unit) },
          parameters = 0,
        ).value
      }
      assertContains(requireNotNull(failure.message), "cannot be called from within a transaction")
    }

    assertEquals(0, connectionPool.setJournalModeCalls)
    transacter.transaction {}
  }

  @Test
  fun `commit failure after execution clears transaction state and fails writer closed`() {
    val connectionPool = installDriver(EndTransactionFailingConnectionPool())
    assertNull(driver.currentTransaction())
    val baselineWriterReleaseCount = connectionPool.writerReleaseCount
    connectionPool.failNext("COMMIT")

    val failure = assertFailsWith<IllegalStateException> {
      transacter.transaction {}
    }

    assertContains(requireNotNull(failure.message), "COMMIT failed")
    assertNull(driver.currentTransaction())
    assertEquals(baselineWriterReleaseCount + 1, connectionPool.writerReleaseCount)
    assertWriterFailsClosed()
  }

  @Test
  fun `rollback failure after execution clears transaction state and fails writer closed`() {
    val connectionPool = installDriver(EndTransactionFailingConnectionPool())
    assertNull(driver.currentTransaction())
    val baselineWriterReleaseCount = connectionPool.writerReleaseCount
    connectionPool.failNext("ROLLBACK")

    val failure = assertFailsWith<IllegalStateException> {
      transacter.transaction { rollback() }
    }

    assertContains(requireNotNull(failure.message), "ROLLBACK failed")
    assertNull(driver.currentTransaction())
    assertEquals(baselineWriterReleaseCount + 1, connectionPool.writerReleaseCount)
    assertWriterFailsClosed()
  }

  @Test
  fun `commit failure rolls back the active transaction before releasing writer`() {
    val connectionPool = installDriver(BeforeEndTransactionFailingConnectionPool())
    assertNull(driver.currentTransaction())
    connectionPool.failNext("COMMIT")

    val failure = assertFailsWith<IllegalStateException> {
      transacter.transaction {}
    }
    assertContains(requireNotNull(failure.message), "COMMIT failed before execution")
    assertNull(driver.currentTransaction())
    assertEquals(1, connectionPool.rollbackAttempts)
    transacter.transaction {}
  }

  @Test
  fun `commit and recovery rollback failure make the writer fail closed`() {
    val connectionPool = installDriver(BeforeEndTransactionFailingConnectionPool())
    assertNull(driver.currentTransaction())
    connectionPool.failNext("COMMIT", "ROLLBACK")

    val failure = assertFailsWith<IllegalStateException> {
      transacter.transaction {}
    }
    assertContains(requireNotNull(failure.message), "COMMIT failed before execution")
    assertContains(
      requireNotNull(failure.suppressedExceptions.single().message),
      "ROLLBACK failed before execution",
    )
    assertNull(driver.currentTransaction())
    assertWriterFailsClosed()
  }

  @Test
  fun `rollback failure retries rollback before releasing writer`() {
    val connectionPool = installDriver(BeforeEndTransactionFailingConnectionPool())
    assertNull(driver.currentTransaction())
    connectionPool.failNext("ROLLBACK")

    val failure = assertFailsWith<IllegalStateException> {
      transacter.transaction { rollback() }
    }

    assertContains(requireNotNull(failure.message), "ROLLBACK failed before execution")
    assertNull(driver.currentTransaction())
    assertEquals(2, connectionPool.rollbackAttempts)
    transacter.transaction {}
  }

  @Test
  fun `rollback and recovery rollback failure make the writer fail closed`() {
    val connectionPool = installDriver(BeforeEndTransactionFailingConnectionPool())
    assertNull(driver.currentTransaction())
    connectionPool.failNext("ROLLBACK", "ROLLBACK")

    val failure = assertFailsWith<IllegalStateException> {
      transacter.transaction { rollback() }
    }

    assertContains(requireNotNull(failure.message), "ROLLBACK failed before execution")
    assertContains(
      requireNotNull(failure.suppressedExceptions.single().message),
      "ROLLBACK failed before execution",
    )
    assertNull(driver.currentTransaction())
    assertWriterFailsClosed()
  }

  private fun assertWriterFailsClosed() {
    val failure = assertFailsWith<IllegalStateException> {
      transacter.transaction {}
    }
    assertContains(requireNotNull(failure.message), "unusable after failed transaction recovery")
  }

  @Test
  fun afterCommitRunsAfterTransactionCommits() {
    var counter = 0
    transacter.transaction {
      afterCommit { counter++ }
      assertEquals(0, counter)
    }

    assertEquals(1, counter)
  }

  @Test
  fun afterCommitDoesNotRunAfterTransactionRollbacks() {
    var counter = 0
    transacter.transaction {
      afterCommit { counter++ }
      assertEquals(0, counter)
      rollback()
    }

    assertEquals(0, counter)
  }

  @Test
  fun afterCommitRunsAfterEnclosingTransactionCommits() {
    var counter = 0
    transacter.transaction {
      afterCommit { counter++ }
      assertEquals(0, counter)

      transaction {
        afterCommit { counter++ }
        assertEquals(0, counter)
      }

      assertEquals(0, counter)
    }

    assertEquals(2, counter)
  }

  @Test
  fun afterCommitDoesNotRunInNestedTransactionWhenEnclosingRollsBack() {
    var counter = 0
    transacter.transaction {
      afterCommit { counter++ }
      assertEquals(0, counter)

      transaction {
        afterCommit { counter++ }
      }

      rollback()
    }

    assertEquals(0, counter)
  }

  @Test
  fun afterCommitDoesNotRunInNestedTransactionWhenNestedRollsBack() {
    var counter = 0
    transacter.transaction {
      afterCommit { counter++ }
      assertEquals(0, counter)

      transaction {
        afterCommit { counter++ }
        rollback()
      }

      throw AssertionError()
    }

    assertEquals(0, counter)
  }

  @Test
  fun afterRollbackNoOpsIfTheTransactionNeverRollsBack() {
    var counter = 0
    transacter.transaction {
      afterRollback { counter++ }
    }

    assertEquals(0, counter)
  }

  @Test
  fun afterRollbackRunsAfterARollbackOccurs() {
    var counter = 0
    transacter.transaction {
      afterRollback { counter++ }
      rollback()
    }

    assertEquals(1, counter)
  }

  @Test
  fun afterRollbackRunsAfterAnInnerTransactionRollsBack() {
    var counter = 0
    transacter.transaction {
      afterRollback { counter++ }
      transaction {
        rollback()
      }
      throw AssertionError()
    }

    assertEquals(1, counter)
  }

  @Test
  fun afterRollbackRunsInAnInnerTransactionWhenTheOuterTransactionRollsBack() {
    var counter = 0
    transacter.transaction {
      transaction {
        afterRollback { counter++ }
      }
      rollback()
    }

    assertEquals(1, counter)
  }

  @Test
  fun transactionsCloseThemselvesOutProperly() {
    var counter = 0
    transacter.transaction {
      afterCommit { counter++ }
    }

    transacter.transaction {
      afterCommit { counter++ }
    }

    assertEquals(2, counter)
  }

  @Test
  fun settingNoEnclosingFailsIfThereIsACurrentlyRunningTransaction() {
    transacter.transaction(noEnclosing = true) {
      assertFailsWith<IllegalStateException> {
        transacter.transaction(noEnclosing = true) {
          throw AssertionError()
        }
      }
    }
  }

  @Test
  fun anExceptionThrownInPostRollbackFunctionIsCombinedWithTheExceptionInTheMainBody() {
    class ExceptionA : RuntimeException()
    class ExceptionB : RuntimeException()

    val t = assertFailsWith<Throwable> {
      transacter.transaction {
        afterRollback {
          throw ExceptionA()
        }
        throw ExceptionB()
      }
    }
    assertTrue("Exception thrown in body not in message($t)") { t.toString().contains("ExceptionA") }
    assertTrue("Exception thrown in rollback not in message($t)") { t.toString().contains("ExceptionB") }
  }

  @Test
  fun weCanReturnAValueFromATransaction() {
    val result: String = transacter.transactionWithResult { "sup" }

    assertEquals(result, "sup")
  }

  @Test
  fun weCanRollbackWithValueFromATransaction() {
    val result: String = transacter.transactionWithResult {
      rollback("rollback")

      @Suppress("UNREACHABLE_CODE")
      "sup"
    }

    assertEquals(result, "rollback")
  }

  @Test
  fun `detect the afterRollback call has escaped the original transaction thread in transaction`() {
    assertChecksThreadConfinement(
      transacter = transacter,
      scope = { transaction(false, it) },
      block = { afterRollback {} },
    )
  }

  @Test
  fun `detect the afterCommit call has escaped the original transaction thread in transaction`() {
    assertChecksThreadConfinement(
      transacter = transacter,
      scope = { transaction(false, it) },
      block = { afterCommit {} },
    )
  }

  @Test
  fun `detect the rollback call has escaped the original transaction thread in transaction`() {
    assertChecksThreadConfinement(
      transacter = transacter,
      scope = { transaction(false, it) },
      block = { rollback() },
    )
  }

  @Test
  fun `detect the afterRollback call has escaped the original transaction thread in transactionWithReturn`() {
    assertChecksThreadConfinement(
      transacter = transacter,
      scope = { transactionWithResult(false, it) },
      block = { afterRollback {} },
    )
  }

  @Test
  fun `detect the afterCommit call has escaped the original transaction thread in transactionWithReturn`() {
    assertChecksThreadConfinement(
      transacter = transacter,
      scope = { transactionWithResult(false, it) },
      block = { afterCommit {} },
    )
  }

  @Test
  fun `detect the rollback call has escaped the original transaction thread in transactionWithReturn`() {
    assertChecksThreadConfinement(
      transacter = transacter,
      scope = { transactionWithResult(false, it) },
      block = { rollback(Unit) },
    )
  }

  private fun <Pool : ConnectionPool> installDriver(connectionPool: Pool): Pool {
    driver.close()
    driver = setupDatabase(
      schema = object : SqlSchema<QueryResult.Value<Unit>> {
        override val version = 1L
        override fun create(driver: SqlDriver): QueryResult.Value<Unit> = QueryResult.Unit
        override fun migrate(
          driver: SqlDriver,
          oldVersion: Long,
          newVersion: Long,
          vararg callbacks: AfterVersion,
        ): QueryResult.Value<Unit> = QueryResult.Unit
      },
      connectionPool = connectionPool,
    )
    transacter = object : TransacterImpl(driver) {}
    return connectionPool
  }
}

private class JournalModeTransactionGuardConnectionPool : ConnectionPool {
  private val connection = androidxSqliteTestDriver().open(":memory:")
  private val writerMutex = Mutex()
  var setJournalModeCalls = 0
    private set

  override fun acquireWriterConnection(): SQLiteConnection = runBlocking {
    writerMutex.lock()
    connection
  }

  override fun releaseWriterConnection() {
    writerMutex.unlock()
  }

  override fun invalidateWriterConnection(cause: Throwable) = error("Not used")

  override fun acquireReaderConnection(): SQLiteConnection = acquireWriterConnection()

  override fun releaseReaderConnection(connection: SQLiteConnection) {
    releaseWriterConnection()
  }

  override fun <R> setJournalMode(
    executeStatement: (SQLiteConnection) -> QueryResult.Value<R>,
  ): QueryResult.Value<R> {
    setJournalModeCalls++
    error("ConnectionPool.setJournalMode must not be called from a transaction")
  }

  override fun close() {
    connection.close()
  }
}

private class EndTransactionFailingConnectionPool : ConnectionPool {
  private var failingStatement: String? = null
  private var terminalFailure: Throwable? = null
  private val delegate = androidxSqliteTestDriver().open(":memory:")
  private val writerMutex = Mutex()
  private val connection = object : SQLiteConnection by delegate {
    override fun prepare(sql: String): SQLiteStatement {
      val statement = delegate.prepare(sql)
      if(sql != failingStatement) return statement

      failingStatement = null
      return object : SQLiteStatement by statement {
        override fun step(): Boolean {
          statement.step()
          error("$sql failed after execution")
        }
      }
    }
  }

  var writerReleaseCount = 0
    private set

  fun failNext(statement: String) {
    check(failingStatement == null)
    failingStatement = statement
  }

  override fun acquireWriterConnection(): SQLiteConnection = runBlocking {
    terminalFailure?.let { cause ->
      throw IllegalStateException(
        "SQLite writer is unusable after failed transaction recovery",
        cause,
      )
    }
    writerMutex.lock()
    connection
  }

  override fun releaseWriterConnection() {
    writerReleaseCount++
    writerMutex.unlock()
  }

  override fun invalidateWriterConnection(cause: Throwable) {
    terminalFailure = cause
  }

  override fun acquireReaderConnection(): SQLiteConnection = acquireWriterConnection()

  override fun releaseReaderConnection(connection: SQLiteConnection) {
    releaseWriterConnection()
  }

  override fun <R> setJournalMode(
    executeStatement: (SQLiteConnection) -> QueryResult.Value<R>,
  ): QueryResult.Value<R> = error("Don't use")

  override fun close() {
    delegate.close()
  }
}

private class BeforeEndTransactionFailingConnectionPool : ConnectionPool {
  private val failingStatements = ArrayDeque<String>()
  private var terminalFailure: Throwable? = null
  private val delegate = androidxSqliteTestDriver().open(":memory:")
  private val writerMutex = Mutex()
  private val connection = object : SQLiteConnection by delegate {
    override fun prepare(sql: String): SQLiteStatement {
      if(sql == "ROLLBACK") rollbackAttempts++
      val statement = delegate.prepare(sql)
      if(failingStatements.firstOrNull() != sql) return statement

      failingStatements.removeFirst()
      return object : SQLiteStatement by statement {
        override fun step(): Boolean = error("$sql failed before execution")
      }
    }
  }

  var rollbackAttempts = 0
    private set

  fun failNext(vararg statements: String) {
    check(failingStatements.isEmpty())
    failingStatements.addAll(statements)
  }

  override fun acquireWriterConnection(): SQLiteConnection = runBlocking {
    terminalFailure?.let { cause ->
      throw IllegalStateException(
        "SQLite writer is unusable after failed transaction recovery",
        cause,
      )
    }
    writerMutex.lock()
    connection
  }

  override fun releaseWriterConnection() {
    writerMutex.unlock()
  }

  override fun invalidateWriterConnection(cause: Throwable) {
    terminalFailure = cause
  }

  override fun acquireReaderConnection(): SQLiteConnection = acquireWriterConnection()

  override fun releaseReaderConnection(connection: SQLiteConnection) {
    releaseWriterConnection()
  }

  override fun <R> setJournalMode(
    executeStatement: (SQLiteConnection) -> QueryResult.Value<R>,
  ): QueryResult.Value<R> = error("Don't use")

  override fun close() {
    delegate.close()
  }
}

private class FirstUserTransactionFailsConnectionPool : ConnectionPool {
  private val writerMutex = Mutex()
  private val writerOwned = atomic(false)

  private val firstTransactionFailConnection = object : SQLiteConnection {
    private var beginTransactionAttempts = 0

    private val connection = androidxSqliteTestDriver().open(":memory:")

    override fun close() {
      connection.close()
    }

    override fun prepare(sql: String) =
      if(sql == "BEGIN IMMEDIATE" && ++beginTransactionAttempts == 2) {
        error("Throwing an error")
      }
      else {
        connection.prepare(sql)
      }
  }

  override fun close() {
    firstTransactionFailConnection.close()
  }

  override fun acquireWriterConnection(): SQLiteConnection = runBlocking {
    writerMutex.lock()
    check(writerOwned.compareAndSet(expect = false, update = true))
    firstTransactionFailConnection
  }

  override fun releaseWriterConnection() {
    check(writerOwned.compareAndSet(expect = true, update = false))
    writerMutex.unlock()
  }
  override fun invalidateWriterConnection(cause: Throwable) = error("Not used")
  override fun acquireReaderConnection() = firstTransactionFailConnection
  override fun releaseReaderConnection(connection: SQLiteConnection) {}
  override fun <R> setJournalMode(
    executeStatement: (SQLiteConnection) -> QueryResult.Value<R>,
  ): QueryResult.Value<R> = error("Don't use")

  fun recoverLeakedWriterForTestCleanup() {
    if(writerOwned.value) releaseWriterConnection()
  }
}
