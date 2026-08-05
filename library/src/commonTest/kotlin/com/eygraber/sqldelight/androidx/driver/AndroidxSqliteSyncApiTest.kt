package com.eygraber.sqldelight.androidx.driver

import app.cash.sqldelight.db.AfterVersion
import app.cash.sqldelight.db.QueryResult
import app.cash.sqldelight.db.SqlCursor
import app.cash.sqldelight.db.SqlDriver
import app.cash.sqldelight.db.SqlSchema
import kotlin.test.Test
import kotlin.test.assertEquals

private fun testSchema() = object : SqlSchema<QueryResult.Value<Unit>> {
  override val version: Long = 1

  override fun create(driver: SqlDriver): QueryResult.Value<Unit> {
    driver.execute(
      identifier = null,
      sql = "CREATE TABLE test(value TEXT NOT NULL)",
      parameters = 0,
    ).value
    return QueryResult.Unit
  }

  override fun migrate(
    driver: SqlDriver,
    oldVersion: Long,
    newVersion: Long,
    vararg callbacks: AfterVersion,
  ): QueryResult.Value<Unit> = QueryResult.Unit
}

abstract class AndroidxSqliteSyncApiTest {
  @Test
  fun synchronousSqlDriverContract() {
    val driver: SqlDriver = AndroidxSqliteDriver(
      driver = androidxSqliteTestDriver(),
      databaseType = AndroidxSqliteDatabaseType.Memory,
      schema = testSchema(),
    )

    driver.execute(null, "INSERT INTO test(value) VALUES (?)", 1) {
      bindString(0, "sync")
    }.value
    val value = driver.executeQuery(
      null,
      "SELECT value FROM test",
      { cursor: SqlCursor ->
        check(cursor.next().value)
        QueryResult.Value(cursor.getString(0))
      },
      0,
    ).value

    assertEquals("sync", value)
    driver.close()
  }
}
