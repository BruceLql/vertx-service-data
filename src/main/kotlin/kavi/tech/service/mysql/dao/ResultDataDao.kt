package kavi.tech.service.mysql.dao

import io.vertx.core.json.JsonObject
import io.vertx.core.logging.Logger
import io.vertx.ext.sql.ResultSet
import io.vertx.rxjava.ext.asyncsql.AsyncSQLClient
import kavi.tech.service.common.extension.logger
import kavi.tech.service.mysql.component.AbstractDao
import kavi.tech.service.mysql.component.SQL
import kavi.tech.service.mysql.entity.QueryRecords
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
    public fun insert(resultData: ResultData): Single<Void> {
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
    public fun selectData(where: List<Serializable>): Single<JsonObject> {
        val sql = SQL.init { SELECT("*"); FROM(ResultData.tableName); WHERES(where) }
        print("selectData sql:$sql")
        return this.one(sql)

    }


    /**
     * 查询最近一次的采集结果的任务ID
     */
    fun queryLastestTaskId(queryRecord: QueryRecords): Single<ResultSet> {
        var taskIdSqlStr = ""
        if (!queryRecord.task_id.isNullOrEmpty()) taskIdSqlStr = "and task_id = '${queryRecord.task_id}' "

        val sql = "SELECT task_id " +
                "FROM ${ResultData.tableName} " +
                "WHERE `mobile` = '${queryRecord.mobile}' " +
                taskIdSqlStr +
                "ORDER BY created_at DESC  limit 1\n"

        println("queryLastestTaskId:$sql")


        return this.client.rxGetConnection().flatMap { conn ->

            conn.rxQuery(sql).doAfterTerminate(conn::close)

        }
    }

}
