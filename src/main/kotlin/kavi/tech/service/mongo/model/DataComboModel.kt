package kavi.tech.service.mongo.model


import io.vertx.core.json.JsonObject
import io.vertx.ext.mongo.FindOptions
import io.vertx.rxjava.ext.mongo.MongoClient
import kavi.tech.service.mongo.component.AbstractModel
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import kavi.tech.service.mongo.schema.DataCombo
import rx.Single


@Repository
class DataComboModel @Autowired constructor(val client: MongoClient) :
    AbstractModel<DataCombo>(client, DataCombo.TABLE_NAME, DataCombo::class.java) {

    override val log = kavi.tech.service.common.extension.logger(this::class)

    private val tableName = DataCombo.TABLE_NAME

    fun queryListAndSave2Mysql(query: JsonObject): Single<List<JsonObject>> {
        val startTime = System.currentTimeMillis()
        return this.client.rxFindWithOptions(tableName, query, FindOptions())
            .doAfterTerminate { logger(query, startTime) }
    }

}
