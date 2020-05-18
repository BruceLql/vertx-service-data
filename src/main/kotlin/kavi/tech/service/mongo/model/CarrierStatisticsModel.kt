package kavi.tech.service.mongo.model


import com.mongodb.BasicDBObject
import kavi.tech.service.common.extension.value
import kavi.tech.service.mongo.component.AbstractModel
import kavi.tech.service.mongo.component.PageItemModel
import kavi.tech.service.mongo.schema.CarrierStatistics
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.rxjava.ext.mongo.MongoClient
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import rx.Observable
import rx.Single
import java.math.BigDecimal
import kotlin.math.roundToInt


@Repository
class CarrierStatisticsModel @Autowired constructor(val client: MongoClient) :
    AbstractModel<CarrierStatistics>(client, CarrierStatistics.TABLE_NAME, CarrierStatistics::class.java) {

    override val log = kavi.tech.service.common.extension.logger(this::class)

    private val tableName = CarrierStatistics.TABLE_NAME
    /**
     * 保存记录
     */
    fun saveCarrierStatistics(carrierStatistics: CarrierStatistics): Single<CarrierStatistics> {
        carrierStatistics.preInsert()
      return  add(carrierStatistics)
    }

    /**
     * 查询
     */
    fun queryCarrierStatistics(query: JsonObject,fields: JsonObject): Single<JsonObject>{
       return this.client.rxFindOne(tableName,query,fields)
    }


    /**
     * 查询运营商汇总数据
     *  @param jsonArray 查询条件（时间区间）
     */
    fun queryTotalData(jsonArray: JsonArray): Observable<MutableList<JsonObject>> {
        val startTime = System.currentTimeMillis()
        println("jsonArray mongo 查询条件：$jsonArray")

        return this.client.aggregate(tableName, jsonArray).toObservable().toList()
            .doAfterTerminate { logger(jsonArray, startTime) }
    }

    /**
     * 按运营商类型、城市分组 aggregate 管道查询分页
     */
    fun aggregateListPage(
        dateStart: String,
        dateEnd: String,
        page: Int,
        size: Int
    ): Single<PageItemModel<JsonObject>> {
        val query = JsonObject()

        query.put(
            "created_at",
            BasicDBObject("\$gte", JsonObject().put("\$date", dateStart.replace(" ", "T") + "Z")).append(
                "\$lte",
                JsonObject().put("\$date", dateEnd.replace(" ", "T") + "Z")
            )
        )
        val all = BasicDBObject("_id", JsonObject().put("operator", "\$operator").put("city", "\$city"))
            .append("avg", BasicDBObject("\$avg", "\$statistics"))
            .append("max", BasicDBObject("\$max", "\$statistics"))
            .append("min", BasicDBObject("\$min", "\$statistics"))
            .append("count", BasicDBObject("\$sum", 1))
            .append(
                "sucCount", BasicDBObject(
                    "\$sum",
                    BasicDBObject(
                        "\$cond",
                        JsonArray().add(
                            BasicDBObject(
                                "\$ne",
                                JsonArray().add("\$success").add(true)
                            )
                        ).add(0).add(1)
                    )
                )
            )
        val match = BasicDBObject("\$match", query)
        val group = BasicDBObject("\$group", all)
        val sort = BasicDBObject("\$sort", JsonObject().put("_id.city", -1).put("_id.operator", -1))
        val skip = BasicDBObject("\$skip", (page - 1) * size)
        val limit = BasicDBObject("\$limit", size)
        val jsonArray = JsonArray()
        jsonArray.add(match)
        jsonArray.add(group)
        jsonArray.add(sort)
        jsonArray.add(skip)
        jsonArray.add(limit)
        val jsonArrayCount = JsonArray()
        val groupCount =
            BasicDBObject("\$group", BasicDBObject("_id", null).append("totalCount", BasicDBObject("\$sum", 1)))
        jsonArrayCount.add(match).add(group).add(groupCount)
        val countObservable = queryTotalData(jsonArrayCount).toSingle()
        val dataObservable = queryTotalData(jsonArray).toSingle()

        return Single.zip(countObservable, dataObservable) { count, data ->

            val items = data.toList().mapNotNull {
                if (!it.value<JsonObject>("_id")?.value<String>("operator").isNullOrEmpty()) {
                    it.value<JsonObject>("_id")?.mapNotNull { id_ ->
                        it.put(id_.key, id_.value)
                    }
                    it.remove("_id")
                }
                var sucRat = 0.00
                val countValue = it.value<Int>("count") ?: 0
                val sucCount = it.value<Int>("sucCount") ?: 0

                it.put("avg", (it.getValue("avg") as Double).roundToInt())
                if (countValue > 0) sucRat =
                    BigDecimal(sucCount * 100).divide(BigDecimal(countValue), 2, BigDecimal.ROUND_HALF_UP)
                        .toDouble()
                it.put("sucRat", sucRat)
            }
            var _count: Long = 0
            println("========================count : ${count.toString()} ")

            if (count.size > 0) {
                _count = count[0].value<Int>("totalCount")?.toLong()!!
            }
            PageItemModel(_count, size, items)

        }
    }
}
