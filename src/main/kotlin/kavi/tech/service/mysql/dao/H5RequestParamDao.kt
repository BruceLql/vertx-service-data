package kavi.tech.service.mysql.dao

import io.vertx.core.logging.Logger
import io.vertx.rxjava.ext.asyncsql.AsyncSQLClient
import kavi.tech.service.common.extension.logger
import kavi.tech.service.mysql.component.AbstractDao
import kavi.tech.service.mysql.component.SQL
import kavi.tech.service.mysql.entity.H5RequestParam
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import rx.Single
import java.io.Serializable

@Repository
class H5RequestParamDao @Autowired constructor(
    private val client: AsyncSQLClient
) : AbstractDao<H5RequestParam>(client) {
    override val log: Logger = logger(this::class)



    fun queryParams(where: List<Serializable>): Single<H5RequestParam> {
        val sql  =SQL.init { SELECT("*"); FROM(H5RequestParam.tableName);WHERES(where) }
        log.info("===========queryParams sql:$sql")
        return this.one(sql)
    }



}
