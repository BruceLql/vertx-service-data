package kavi.tech.service.mysql.dao

import io.vertx.core.logging.Logger
import io.vertx.rxjava.ext.asyncsql.AsyncSQLClient
import kavi.tech.service.common.extension.logger
import kavi.tech.service.mysql.component.AbstractDao
import kavi.tech.service.mysql.component.SQL
import kavi.tech.service.mysql.entity.ResultData
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import rx.Single
import java.io.Serializable

@Repository
class ResultDataDao @Autowired constructor(
    private val client: AsyncSQLClient
) : AbstractDao<ResultData>(client) {
    override val log: Logger = logger(this::class)


    /**
     * 新增记录
     * */
    private fun insert(resultData: ResultData): Single<Void> {
        val sql = SQL.init {
            INSERT_INTO(resultData.tableName())
            resultData.preInsert().forEach { (t, u) -> VALUES(t, u) }
        }
        println("insert sql：$sql")
        return this.client.rxGetConnection().flatMap { conn ->

            conn.rxExecute(sql).doAfterTerminate(conn::close)

        }
    }

    /**
     * 查询已清洗的数据结果
     * */
    private fun selectData(where: List<Serializable>): Single<ResultData> {
        return this.one(SQL.init { SELECT("*"); FROM(ResultData.tableName); WHERES(where) })
        }




}
