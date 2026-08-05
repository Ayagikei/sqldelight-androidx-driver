package com.eygraber.sqldelight.androidx.driver

import androidx.sqlite.SQLiteConnection
import androidx.sqlite.SQLiteStatement
import app.cash.sqldelight.db.AfterVersion
import app.cash.sqldelight.db.QueryResult
import app.cash.sqldelight.db.SqlDriver
import app.cash.sqldelight.db.SqlSchema
import kotlinx.atomicfu.atomic
import kotlinx.atomicfu.locks.SynchronizedObject
import kotlinx.atomicfu.locks.synchronized
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Job
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import kotlin.random.Random
import kotlin.random.nextULong
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertSame
import kotlin.test.assertTrue

abstract class AndroidxSqliteLifecycleRegressionTest {
  @Test
  fun `concurrent first interactions run lifecycle callbacks exactly once in order`() = runTest {
    val events = mutableListOf<String>()
    val eventsLock = SynchronizedObject()
    val startBarrier = CompletableDeferred<Unit>()
    val callersReady = CompletableDeferred<Unit>()
    val configureEntered = CompletableDeferred<Unit>()
    val releaseConfigure = CompletableDeferred<Unit>()
    val readyCount = atomic(0)
    val schema = schema(
      onCreate = { driver ->
        driver.execute(null, "CREATE TABLE test (id INTEGER PRIMARY KEY)", 0)
      },
    )
    val dbName = testDatabaseName()
    deleteDatabase(dbName)

    val driver = AndroidxSqliteDriver(
      driver = androidxSqliteTestDriver(),
      databaseType = AndroidxSqliteDatabaseType.File(dbName),
      schema = schema,
      onConfigure = {
        synchronized(eventsLock) { events += "configure" }
        configureEntered.complete(Unit)
        runBlocking { releaseConfigure.await() }
      },
      onCreate = { synchronized(eventsLock) { events += "create" } },
      onOpen = { synchronized(eventsLock) { events += "open" } },
    )

    try {
      val jobs = mutableListOf<Job>()
      repeat(32) {
        jobs += launch(IoDispatcher) {
          startBarrier.await()
          if(readyCount.incrementAndGet() == 32) callersReady.complete(Unit)
          callersReady.await()
          driver.execute(null, "INSERT INTO test DEFAULT VALUES", 0)
        }
      }
      startBarrier.complete(Unit)
      configureEntered.await()
      callersReady.await()
      releaseConfigure.complete(Unit)
      jobs.joinAll()

      val lifecycleEvents = synchronized(eventsLock) { events.toList() }
      assertEquals(listOf("configure", "create", "open"), lifecycleEvents)
      assertEquals(1, lifecycleEvents.count { it == "configure" })
      assertEquals(1, lifecycleEvents.count { it == "create" })
      assertEquals(1, lifecycleEvents.count { it == "open" })
    }
    finally {
      driver.close()
      deleteDatabase(dbName)
    }
  }

  @Test
  fun `close is best effort aggregates failures and can retry before becoming idempotent`() {
    val firstStatementFailure = IllegalStateException("first statement close failed")
    val secondStatementFailure = IllegalStateException("second statement close failed")
    val poolFailure = IllegalStateException("pool close failed")
    val pool = RetryableCleanupConnectionPool(
      statementFailures = listOf(firstStatementFailure, secondStatementFailure),
      firstPoolCloseFailure = poolFailure,
    )
    val driver = driverWithOverridingPool(pool)

    driver.cacheQuery(identifier = 1)
    driver.cacheQuery(identifier = 2)

    val failure = assertFailsWith<IllegalStateException> { driver.close() }
    assertSame(firstStatementFailure, failure)
    assertEquals(listOf(secondStatementFailure, poolFailure), failure.suppressedExceptions)
    assertEquals(listOf(1, 1), pool.statementCloseAttempts)
    assertEquals(1, pool.closeAttempts)

    driver.close()
    assertEquals(listOf(2, 2), pool.statementCloseAttempts)
    assertEquals(2, pool.closeAttempts)

    driver.close()
    assertEquals(listOf(2, 2), pool.statementCloseAttempts)
    assertEquals(2, pool.closeAttempts)
  }

  @Test
  fun `same cleanup failure instance does not interrupt remaining cleanup`() {
    val sharedStatementFailure = IllegalStateException("shared statement close failed")
    val poolFailure = IllegalStateException("pool close failed")
    val pool = RetryableCleanupConnectionPool(
      statementFailures = listOf(sharedStatementFailure, sharedStatementFailure),
      firstPoolCloseFailure = poolFailure,
    )
    val driver = driverWithOverridingPool(pool)

    driver.cacheQuery(identifier = 1)
    driver.cacheQuery(identifier = 2)

    val failure = assertFailsWith<IllegalStateException> { driver.close() }
    assertSame(sharedStatementFailure, failure)
    assertEquals(listOf(poolFailure), failure.suppressedExceptions)
    assertEquals(listOf(1, 1), pool.statementCloseAttempts)
    assertEquals(1, pool.closeAttempts)

    driver.close()
  }

  @Test
  fun `configure failure remains primary when cleanup fails`() {
    val configureFailure = IllegalStateException("configure failed")
    val cleanupFailure = IllegalStateException("cleanup failed")
    val pool = AlwaysFailingClosePool(cleanupFailure)
    val driver = driverWithOverridingPool(
      pool = pool,
      onConfigure = { throw configureFailure },
    )

    val failure = assertFailsWith<IllegalStateException> {
      driver.execute(null, "SELECT 1", 0)
    }

    assertSame(configureFailure, failure)
    assertEquals(listOf(cleanupFailure), failure.suppressedExceptions)
    assertEquals(1, pool.closeAttempts)
  }

  @Test
  fun `migration failure remains primary when cleanup fails`() {
    val migrationFailure = IllegalStateException("migration failed")
    val cleanupFailure = IllegalStateException("cleanup failed")
    val dbName = testDatabaseName()
    deleteDatabase(dbName)
    val versionOneSchema = schema(onCreate = {})

    val versionOneDriver = AndroidxSqliteDriver(
      driver = androidxSqliteTestDriver(),
      databaseType = AndroidxSqliteDatabaseType.File(dbName),
      schema = versionOneSchema,
    )
    try {
      versionOneDriver.execute(null, "SELECT 1", 0)
    } finally {
      versionOneDriver.close()
    }

    val configuration = AndroidxSqliteConfiguration()
    val delegatePool = AndroidxDriverConnectionPool(
      connectionFactory = androidxSqliteTestConnectionFactory(),
      nameProvider = { dbName },
      isFileBased = true,
      configuration = configuration,
    )
    val pool = FailingClosePool(delegatePool, cleanupFailure)
    val versionTwoSchema = object : SqlSchema<QueryResult.Value<Unit>> {
      override val version = 2L

      override fun create(driver: SqlDriver) = QueryResult.Unit

      override fun migrate(
        driver: SqlDriver,
        oldVersion: Long,
        newVersion: Long,
        vararg callbacks: AfterVersion,
      ): QueryResult.Value<Unit> = throw migrationFailure
    }
    val driver = AndroidxSqliteDriver(
      connectionFactory = androidxSqliteTestConnectionFactory(),
      databaseType = AndroidxSqliteDatabaseType.File(dbName),
      schema = versionTwoSchema,
      configuration = configuration,
      overridingConnectionPool = pool,
    )

    try {
      val failure = assertFailsWith<IllegalStateException> {
        driver.execute(null, "SELECT 1", 0)
      }
      assertSame(migrationFailure, failure)
      assertEquals(listOf(cleanupFailure), failure.suppressedExceptions)
      assertEquals(1, pool.closeAttempts)
    }
    finally {
      deleteDatabase(dbName)
    }
  }

  @Test
  fun `tracking connection reports closed only after delegate close succeeds`() {
    val closeFailure = IllegalStateException("close failed")
    val delegate = object : SQLiteConnection {
      override fun prepare(sql: String): SQLiteStatement = error("Not used")
      override fun close(): Unit = throw closeFailure
    }
    val connection = TrackingConnection(delegate)

    assertSame(closeFailure, assertFailsWith<IllegalStateException> { connection.close() })
    assertTrue(!connection.isClosed)
  }

  @Test
  fun `configure failure closes materialized connection and a new driver can retry`() {
    var createCount = 0
    val schema = schema(
      onCreate = { driver ->
        createCount++
        driver.execute(null, "CREATE TABLE test (id INTEGER PRIMARY KEY)", 0)
      },
    )
    val dbName = testDatabaseName()
    deleteDatabase(dbName)
    val trackingFactory = TrackingConnectionFactory()
    val failingDriver = AndroidxSqliteDriver(
      connectionFactory = trackingFactory,
      databaseType = AndroidxSqliteDatabaseType.File(dbName),
      schema = schema,
      onConfigure = {
        setForeignKeyConstraintsEnabled(true)
        throw IllegalStateException("configure failed")
      },
    )

    try {
      val failure = assertFailsWith<IllegalStateException> {
        failingDriver.execute(null, "SELECT 1", 0)
      }
      assertEquals("configure failed", failure.message)
      assertEquals(0, createCount)
      assertTrue(trackingFactory.connections.isNotEmpty())
      assertTrue(trackingFactory.connections.all(TrackingConnection::isClosed))
      failingDriver.close()

      val retryingDriver = AndroidxSqliteDriver(
        driver = androidxSqliteTestDriver(),
        databaseType = AndroidxSqliteDatabaseType.File(dbName),
        schema = schema,
      )
      try {
        retryingDriver.execute(null, "INSERT INTO test DEFAULT VALUES", 0)
      } finally {
        retryingDriver.close()
      }

      assertEquals(1, createCount)
    }
    finally {
      deleteDatabase(dbName)
    }
  }

  @Test
  fun `opening a newer database with an older schema fails without modifying the database`() {
    val dbName = testDatabaseName()
    deleteDatabase(dbName)
    val versionTwoSchema = versionedSchema(version = 2L)
    val creatingDriver = AndroidxSqliteDriver(
      driver = androidxSqliteTestDriver(),
      databaseType = AndroidxSqliteDatabaseType.File(dbName),
      schema = versionTwoSchema,
    )

    try {
      creatingDriver.execute(
        identifier = null,
        sql = "INSERT INTO test(value) VALUES ('preserved')",
        parameters = 0,
      ).value
    }
    finally {
      creatingDriver.close()
    }

    var migrateCalled = false
    var openCalled = false
    val olderSchema = object : SqlSchema<QueryResult.Value<Unit>> {
      override val version = 1L

      override fun create(driver: SqlDriver) = error("create must not run during downgrade")

      override fun migrate(
        driver: SqlDriver,
        oldVersion: Long,
        newVersion: Long,
        vararg callbacks: AfterVersion,
      ): QueryResult.Value<Unit> {
        migrateCalled = true
        return QueryResult.Unit
      }
    }
    val olderDriver = AndroidxSqliteDriver(
      driver = androidxSqliteTestDriver(),
      databaseType = AndroidxSqliteDatabaseType.File(dbName),
      schema = olderSchema,
      onOpen = { openCalled = true },
    )

    try {
      val failure = assertFailsWith<IllegalStateException> {
        olderDriver.execute(null, "SELECT value FROM test", 0).value
      }
      assertEquals(
        "Database version 2 is newer than schema version 1; downgrades are not supported",
        failure.message,
      )
      assertTrue(!migrateCalled)
      assertTrue(!openCalled)

      val verifyingDriver = AndroidxSqliteDriver(
        driver = androidxSqliteTestDriver(),
        databaseType = AndroidxSqliteDatabaseType.File(dbName),
        schema = versionTwoSchema,
      )
      try {
        val version = verifyingDriver.executeQuery(
          identifier = null,
          sql = "PRAGMA user_version",
          mapper = { cursor ->
            check(cursor.next().value)
            QueryResult.Value(cursor.getLong(0))
          },
          parameters = 0,
        ).value
        val value = verifyingDriver.executeQuery(
          identifier = null,
          sql = "SELECT value FROM test",
          mapper = { cursor ->
            check(cursor.next().value)
            QueryResult.Value(cursor.getString(0))
          },
          parameters = 0,
        ).value

        assertEquals(2L, version)
        assertEquals("preserved", value)
      }
      finally {
        verifyingDriver.close()
      }
    }
    finally {
      runCatching { olderDriver.close() }
      deleteDatabase(dbName)
    }
  }

  private fun schema(
    onCreate: (SqlDriver) -> Unit,
  ) = object : SqlSchema<QueryResult.Value<Unit>> {
    override val version = 1L

    override fun create(driver: SqlDriver): QueryResult.Value<Unit> {
      onCreate(driver)
      return QueryResult.Unit
    }

    override fun migrate(
      driver: SqlDriver,
      oldVersion: Long,
      newVersion: Long,
      vararg callbacks: AfterVersion,
    ) = QueryResult.Unit
  }

  private fun versionedSchema(
    version: Long,
  ) = object : SqlSchema<QueryResult.Value<Unit>> {
    override val version = version

    override fun create(driver: SqlDriver): QueryResult.Value<Unit> {
      driver.execute(
        identifier = null,
        sql = "CREATE TABLE test (value TEXT NOT NULL)",
        parameters = 0,
      ).value
      return QueryResult.Unit
    }

    override fun migrate(
      driver: SqlDriver,
      oldVersion: Long,
      newVersion: Long,
      vararg callbacks: AfterVersion,
    ) = QueryResult.Unit
  }

  private fun testDatabaseName() =
    "${this::class.qualifiedName}.${Random.nextULong().toHexString()}.db"

  private fun deleteDatabase(dbName: String) {
    deleteFile(dbName)
    deleteFile("$dbName-shm")
    deleteFile("$dbName-wal")
  }

  private fun driverWithOverridingPool(
    pool: ConnectionPool,
    onConfigure: AndroidxSqliteConfigurableDriver.() -> Unit = {},
  ) = AndroidxSqliteDriver(
    connectionFactory = androidxSqliteTestConnectionFactory(),
    databaseType = AndroidxSqliteDatabaseType.Memory,
    schema = schema(onCreate = {}),
    configuration = AndroidxSqliteConfiguration(cacheSize = 25),
    onConfigure = onConfigure,
    overridingConnectionPool = pool,
  )

  private fun SqlDriver.cacheQuery(identifier: Int) {
    executeQuery(
      identifier = identifier,
      sql = "SELECT 1",
      mapper = { cursor ->
        cursor.next()
        QueryResult.Unit
      },
      parameters = 0,
      binders = null,
    )
  }

  private class TrackingConnectionFactory : AndroidxSqliteConnectionFactory {
    override val driver = androidxSqliteTestDriver()
    val connections = mutableListOf<TrackingConnection>()

    override fun createConnection(name: String): SQLiteConnection =
      TrackingConnection(driver.open(name)).also(connections::add)
  }

  private class TrackingConnection(
    private val delegate: SQLiteConnection,
  ) : SQLiteConnection by delegate {
    var isClosed = false
      private set

    override fun close() {
      delegate.close()
      isClosed = true
    }
  }

  private class RetryableCleanupConnectionPool(
    statementFailures: List<Throwable>,
    private val firstPoolCloseFailure: Throwable,
  ) : ConnectionPool {
    private val sqliteDriver = androidxSqliteTestDriver()
    private val writer = sqliteDriver.open(":memory:")
    private val statements = statementFailures.mapIndexed { index, failure ->
      RetryableCloseStatementConnection(
        delegate = sqliteDriver.open(":memory:"),
        closeFailure = failure,
        hash = index,
      )
    }
    private var nextReader = 0

    val statementCloseAttempts: List<Int>
      get() = statements.map { it.statementCloseAttempts }

    var closeAttempts = 0
      private set

    override fun acquireWriterConnection() = writer
    override fun releaseWriterConnection() {}
    override fun invalidateWriterConnection(cause: Throwable) = error("Not used")
    override fun acquireReaderConnection() = statements[nextReader++]
    override fun releaseReaderConnection(connection: SQLiteConnection) {}

    override fun <R> setJournalMode(
      executeStatement: (SQLiteConnection) -> QueryResult.Value<R>,
    ): QueryResult.Value<R> = error("Not used")

    override fun close() {
      closeAttempts++
      if(closeAttempts == 1) throw firstPoolCloseFailure
      writer.close()
      statements.forEach { it.close() }
    }
  }

  private class RetryableCloseStatementConnection(
    private val delegate: SQLiteConnection,
    private val closeFailure: Throwable,
    private val hash: Int,
  ) : SQLiteConnection by delegate {
    var statementCloseAttempts = 0
      private set

    override fun prepare(sql: String): SQLiteStatement {
      val statement = delegate.prepare(sql)
      return if(sql == "SELECT 1") {
        object : SQLiteStatement by statement {
          override fun close() {
            statementCloseAttempts++
            if(statementCloseAttempts == 1) throw closeFailure
            statement.close()
          }
        }
      }
      else {
        statement
      }
    }

    override fun hashCode() = hash
  }

  private open class FailingClosePool(
    private val delegate: ConnectionPool,
    private val closeFailure: Throwable,
  ) : ConnectionPool by delegate {
    var closeAttempts = 0
      private set

    override fun close() {
      closeAttempts++
      delegate.close()
      throw closeFailure
    }
  }

  private class AlwaysFailingClosePool(
    closeFailure: Throwable,
  ) : FailingClosePool(
    delegate = AndroidxDriverConnectionPool(
      connectionFactory = androidxSqliteTestConnectionFactory(),
      nameProvider = { ":memory:" },
      isFileBased = false,
      configuration = AndroidxSqliteConfiguration(),
    ),
    closeFailure = closeFailure,
  )
}
