package kavi.tech.service.mongo.model


import io.vertx.core.json.JsonObject
import io.vertx.ext.mongo.FindOptions
import io.vertx.rxjava.ext.mongo.MongoClient
import kavi.tech.service.common.extension.logger
import kavi.tech.service.mongo.component.AbstractModel
import kavi.tech.service.mongo.schema.DataSmsInfo
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import rx.Single

@Repository
class DataSmsInfoModel @Autowired constructor(val client: MongoClient) :
    AbstractModel<DataSmsInfo>(client, DataSmsInfo.TABLE_NAME, DataSmsInfo::class.java) {

    override val log = logger(this::class)

    private val tableName = DataSmsInfo.TABLE_NAME

    fun queryListAndSave2Mysql(query: JsonObject): Single<List<JsonObject>> {
        val startTime = System.currentTimeMillis()
        return this.client.rxFindWithOptions(tableName, query, FindOptions())
            .doAfterTerminate { logger(query, startTime) }
    }

}

