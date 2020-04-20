package kavi.tech.service.mongo.model


import io.vertx.core.json.JsonObject
import io.vertx.ext.mongo.FindOptions
import io.vertx.ext.sql.UpdateResult
import io.vertx.rxjava.ext.mongo.MongoClient
import kavi.tech.service.mongo.component.AbstractModel
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import kavi.tech.service.mongo.schema.DataUserInfo
import kavi.tech.service.mysql.dao.UserInfoDao
import rx.Single

@Repository
class DataUserInfoModel @Autowired constructor(val client: MongoClient) :
    AbstractModel<DataUserInfo>(client, DataUserInfo.TABLE_NAME, DataUserInfo::class.java) {

    override val log = kavi.tech.service.common.extension.logger(this::class)

    private val tableName = DataUserInfo.TABLE_NAME

    fun queryListAndSave2Mysql(query: JsonObject): Single<List<JsonObject>> {
        val startTime = System.currentTimeMillis()
        return this.client.rxFindWithOptions(tableName, query, FindOptions())
            .doAfterTerminate { logger(query, startTime) }
    }
}
