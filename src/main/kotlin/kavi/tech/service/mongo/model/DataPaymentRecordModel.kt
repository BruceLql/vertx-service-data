package kavi.tech.service.mongo.model


import io.vertx.core.json.JsonObject
import io.vertx.ext.mongo.FindOptions
import io.vertx.ext.sql.UpdateResult
import io.vertx.rxjava.ext.mongo.MongoClient
import kavi.tech.service.mongo.component.AbstractModel
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import kavi.tech.service.mongo.schema.DataPaymentRecord
import kavi.tech.service.mysql.dao.PaymentRecordDao
import rx.Single


@Repository
class DataPaymentRecordModel @Autowired constructor(val client: MongoClient,val paymentRecordDao: PaymentRecordDao) :
    AbstractModel<DataPaymentRecord>(client, DataPaymentRecord.TABLE_NAME, DataPaymentRecord::class.java) {

    override val log = kavi.tech.service.common.extension.logger(this::class)

    private val tableName = DataPaymentRecord.TABLE_NAME

    fun queryListAndSave2Mysql(query: JsonObject, findOptions: FindOptions): Single<UpdateResult> {
        val startTime = System.currentTimeMillis()
        return this.client.rxFindWithOptions(tableName, query, findOptions)
            .doAfterTerminate { logger(query, startTime) }.flatMap {
                paymentRecordDao.paymentRecordDataInsert(it)
            }
    }
}
