package kavi.tech.service.mongo.model


import io.vertx.core.json.JsonObject
import io.vertx.ext.mongo.FindOptions
import io.vertx.rxjava.ext.mongo.MongoClient
import kavi.tech.service.mongo.component.AbstractModel
import kavi.tech.service.mongo.schema.CarrierReportInfo
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import rx.Single


@Repository
class CarrierReportInfoModel @Autowired constructor(val client: MongoClient) :
    AbstractModel<CarrierReportInfo>(client, CarrierReportInfo.TABLE_NAME, CarrierReportInfo::class.java) {

    override val log = kavi.tech.service.common.extension.logger(this::class)

    private val tableName = CarrierReportInfo.TABLE_NAME
    /**
     * 查询数据结果
     */
    fun queryListResultData(query: JsonObject, findOptions: FindOptions): Single<List<JsonObject>> {
        println(query.toString() + "-----------------------" + findOptions.toJson())
        val startTime = System.currentTimeMillis()
        return this.client.rxFindWithOptions(tableName, query, findOptions)
            .doAfterTerminate { logger(query, startTime) }
    }

    /**
     *  将运营商报告结果 or 运营商原始数据结果 插入Mongo库
     */
    fun insertReportDataIntoMongo(carrierReportInfo: CarrierReportInfo): Single<CarrierReportInfo> {
        carrierReportInfo.preInsert()
        return add(carrierReportInfo)
    }

    /**
     * 查询最近一次的采集结果的任务ID
     */
    fun queryLatestTaskId(query: JsonObject, findOptions: FindOptions): Single<List<JsonObject>> {

        val startTime = System.currentTimeMillis()
        return this.client.rxFindWithOptions(tableName, query, findOptions)
            .doAfterTerminate { logger(query, startTime) }
    }
}
