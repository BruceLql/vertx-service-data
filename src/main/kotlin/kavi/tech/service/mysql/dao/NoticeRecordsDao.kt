package kavi.tech.service.mysql.dao

import io.vertx.core.logging.Logger
import io.vertx.rxjava.ext.asyncsql.AsyncSQLClient
import kavi.tech.service.common.extension.logger
import kavi.tech.service.mysql.component.AbstractDao
import kavi.tech.service.mysql.component.SQL
import kavi.tech.service.mysql.entity.NoticeRecords
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import rx.Single

@Repository
class NoticeRecordsDao @Autowired constructor(
    private val client: AsyncSQLClient
) : AbstractDao<NoticeRecords>(client) {
    override val log: Logger = logger(this::class)


    /**
     * 新增记录
     * */
    public fun insert(noticeRecords: NoticeRecords): Single<NoticeRecords> {
        val sql = SQL.init {
            INSERT_INTO(noticeRecords.tableName())
            noticeRecords.preInsert().forEach { (t, u) -> VALUES(t, u) }
        }
        println("insert sql：$sql")
        return this.client.rxGetConnection().flatMap { conn ->

            conn.rxUpdate(sql).map {
                noticeRecords.apply { this.id =it.keys.getLong(0) }
            }.doAfterTerminate(conn::close)

        }
    }



}
