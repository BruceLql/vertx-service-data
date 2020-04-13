package kavi.tech.service.mysql.dao

import io.vertx.core.logging.Logger
import io.vertx.rxjava.ext.asyncsql.AsyncSQLClient
import kavi.tech.service.common.extension.logger
import kavi.tech.service.mysql.component.AbstractDao
import kavi.tech.service.mysql.component.SQL
import kavi.tech.service.mysql.entity.QueryRecords
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import rx.Single

@Repository
class QueryRecordsDao @Autowired constructor(
    private val client: AsyncSQLClient
) : AbstractDao<QueryRecords>(client) {
    override val log: Logger = logger(this::class)


    /**
     * 新增记录
     * */
    public fun insert(queryRecords: QueryRecords): Single<QueryRecords> {
        val sql = SQL.init {
            INSERT_INTO(queryRecords.tableName())
            queryRecords.preInsert().forEach { (t, u) -> VALUES(t, u) }
        }
        println("insert sql：$sql")
        return this.client.rxGetConnection().flatMap { conn ->

            conn.rxUpdate(sql).map {
                queryRecords.apply { this.id = it.keys.getLong(0) }
            }.doAfterTerminate(conn::close)

        }
    }


}
