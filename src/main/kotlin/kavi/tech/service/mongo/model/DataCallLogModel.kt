package kavi.tech.service.mongo.model


import io.vertx.core.json.JsonObject
import io.vertx.ext.mongo.FindOptions
import io.vertx.rxjava.ext.mongo.MongoClient
import kavi.tech.service.common.extension.logger
import kavi.tech.service.mongo.component.AbstractModel
import kavi.tech.service.mongo.schema.DataCallLog
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import rx.Single


@Repository
class DataCallLogModel @Autowired constructor(val client: MongoClient) :
    AbstractModel<DataCallLog>(client, DataCallLog.TABLE_NAME, DataCallLog::class.java) {

    override val log = logger(this::class)

    private val tableName = DataCallLog.TABLE_NAME

    fun queryListAndSave2Mysql(query: JsonObject): Single<List<JsonObject>> {
        val startTime = System.currentTimeMillis()
        return this.client.rxFindWithOptions(tableName, query, FindOptions())
               .doAfterTerminate { logger(query, startTime) }
    }

}
