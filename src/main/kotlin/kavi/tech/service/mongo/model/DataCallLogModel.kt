package kavi.tech.service.mongo.model


import io.vertx.core.json.JsonObject
import io.vertx.ext.mongo.FindOptions
import io.vertx.ext.sql.UpdateResult
import io.vertx.rxjava.ext.mongo.MongoClient
import kavi.tech.service.mongo.component.AbstractModel
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import kavi.tech.service.mongo.schema.DataCallLog
import kavi.tech.service.mysql.dao.CallLogDao
import rx.Single


@Repository
class DataCallLogModel @Autowired constructor(val client: MongoClient, val callLogDao: CallLogDao) :
    AbstractModel<DataCallLog>(client, DataCallLog.TABLE_NAME, DataCallLog::class.java) {

    override val log = kavi.tech.service.common.extension.logger(this::class)

    private val tableName = DataCallLog.TABLE_NAME

    fun queryListAndSave2Mysql(query: JsonObject, findOptions: FindOptions): Single<UpdateResult> {
        val startTime = System.currentTimeMillis()
        println("query:$query")
        return this.client.rxFindWithOptions(tableName, query, findOptions).flatMap {
            println("DataCallLogModel queryListAndSave2Mysql:$it")
            callLogDao.callLogDataInsert(it.toList())
        }.doAfterTerminate { logger(query, startTime) }

    }


}
