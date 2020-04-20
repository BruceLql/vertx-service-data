package kavi.tech.service.mongo.model


import io.vertx.core.json.JsonObject
import io.vertx.ext.mongo.FindOptions
import io.vertx.rxjava.ext.mongo.MongoClient
import kavi.tech.service.mongo.component.AbstractModel
import kavi.tech.service.mongo.schema.DataInternetInfo
import kavi.tech.service.mysql.dao.InternetInfoDao
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import rx.Single


@Repository
class DataInternetInfoModel @Autowired constructor(val client: MongoClient, val internetInfoDao: InternetInfoDao) :
    AbstractModel<DataInternetInfo>(client, DataInternetInfo.TABLE_NAME, DataInternetInfo::class.java) {

    override val log = kavi.tech.service.common.extension.logger(this::class)

    private val tableName = DataInternetInfo.TABLE_NAME

    fun queryListAndSave2Mysql(query: JsonObject): Single<List<JsonObject>> {
        val startTime = System.currentTimeMillis()
        return this.client.rxFindWithOptions(tableName, query, FindOptions())
            .doAfterTerminate { logger(query, startTime) }
    }

}
