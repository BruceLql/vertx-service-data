package kavi.tech.service.mysql.dao

import io.vertx.core.json.JsonObject
import io.vertx.core.logging.Logger
import io.vertx.ext.sql.ResultSet
import io.vertx.ext.sql.UpdateResult
import io.vertx.rxjava.ext.asyncsql.AsyncSQLClient
import io.vertx.rxjava.ext.sql.SQLConnection
import kavi.tech.service.mysql.component.AbstractDao
import kavi.tech.service.mysql.component.SQL
import kavi.tech.service.mysql.entity.CarrierResultData
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import rx.Single

/**
 * @packageName kavi.tech.service.mysql.dao
 * @author litiezhu
 * @date 2020/4/13 11:49
 * @Description
 * <a href="goodmanalibaba@foxmail.com"></a>
 * @Versin 1.0
 */
@Repository
class CarrierResultDataDao @Autowired constructor(
    private val client: AsyncSQLClient
) : AbstractDao<CarrierResultData>(client) {
    override val log: Logger = kavi.tech.service.common.extension.logger(this::class)

    /**
     * 新增记录
     * */
     fun insert(conn: SQLConnection, carrierResultData: CarrierResultData): Single<CarrierResultData> {
        val sql = SQL.init {
            INSERT_INTO(carrierResultData.tableName())

            carrierResultData.preInsert().forEach { t, u -> VALUES(t, u) }
        }
        return this.update(conn, sql).map {
            carrierResultData.apply { this.id = it.keys.getLong(0) }
        }
    }

    /**
     * 通过task_id查询数据是否已经入库
     * */
     fun selectBeforeInsert(carrierResultData: CarrierResultData): Single<List<JsonObject>> {

        val sql = SQL.init {
           SELECT("*")
            FROM(CarrierResultData.tableName)
            WHERE(Pair("task_id",carrierResultData.task_id))
            WHERE(Pair("mobile",carrierResultData.mobile))
        }
        log.info("selectBeforeInsert sql：$sql")

        return this.select(sql)
    }

    /**
     * 自定义入参sql
     * */
     fun customizeSQL(sql: String): Single<List<JsonObject>> {
        log.info("selectBeforeInsert sql：$sql")
        return this.select(sql)
    }
    /**
     * 自定义入参sql
     * */
     fun customizeSQLRes(sql: String): Single<ResultSet> {

        log.info("selectBeforeInsert sql：$sql")
        return this.client.rxGetConnection().flatMap { conn ->
            conn.rxQuery(sql).doAfterTerminate(conn::close)

        }
    }

    /**
     * 批量新增通话记录
     * */
    fun insertBybatch(carrierResultData: CarrierResultData, valueList: List<CarrierResultData>): Single<UpdateResult> {

        val sql = SQL.init {
            BATCH_INSERT_INTO(carrierResultData.tableName())
//            BATCH_INTO_COLUMNS( "task_id", "mobile", "bill_month", "time", "peer_number", "location", "location_type", "duration_in_second", "dial_type", "fee", "homearea", "carrier_001", "carrier_002", "created_at", "updated_at", "deleted_at")
            BATCH_INTO_COLUMNS(
                "id",
                "task_id",
                "mobile",
                "item",
                "result",
                "created_at",
                "deleted_at"
            )
            log.info("valueList:$valueList")
            valueList.map {
                val ss = it.preInsert()

                BATCH_INTO_VALUES(
                    ss["id"],
                    '"' + ss["task_id"].toString() + '"',
                    '"' + ss["mobile"].toString() + '"',
                    '"' + ss["item"].toString() + '"',
                    '"' + ss["result"].toString() + '"',
                    '"' + ss["created_at"].toString() + '"',
                    '"' + ss["deleted_at"].toString() + '"'
                )
            }

        }

        log.info("sql=$sql")

        return this.client.rxGetConnection().flatMap { conn ->
            val startTime = System.currentTimeMillis()
            conn.rxUpdate(sql).doAfterTerminate {
                conn.close()
                log.info("执行时间：${System.currentTimeMillis() - startTime}ms")
            }
            /* conn.rxBatch(sql).doAfterTerminate{
                 conn.close()
                 println("执行时间：${System.currentTimeMillis() - startTime}ms")
             }*/
        }

    }

    fun carrierResultDataInsert(data: List<JsonObject>) {

        log.info(" 通话记录存入mysql: .....")
        val carrierResultDataList = ArrayList<CarrierResultData>()
        data.forEach {
            val mobile = it.getString("mobile")
            val task_id = it.getString("mid")
            val dataOut = it.getJsonObject("data")
            if (dataOut.isEmpty) {
                return
            }
            if (dataOut.getInteger("totalNum") < 1) {
                return
            }
            dataOut.getJsonArray("data").forEachIndexed { index, mutableEntry ->
                val carrierResultDatas = CarrierResultData()
                carrierResultDatas.mobile = mobile
                carrierResultDatas.task_id = task_id
                val obj = JsonObject(mutableEntry.toString())
                this.assistantSave(carrierResultDatas, obj)
                carrierResultDataList.add(carrierResultDatas)
            }

        }
        log.info("carrierResultDataList:${carrierResultDataList.size}" + carrierResultDataList.toString())
//        selectBeforeInsert(carrierResultDataList[0]).subscribe({
//            // 如果查询结果的行数大于0 说明已经入库过了  暂时先不处理
//            if (it.numRows == 0) {
//                // 执行批量方法
//                insertBybatch(CarrierResultData(), carrierResultDataList).subscribe({ it ->
//                    println(it)
//                    // todo 处理通话数据分析规则
//                    println("处理分析结果数据表：=====")
//
//                }, {
//                    it.printStackTrace()
//                })
//            } else {
//                log.info("已经存在${it.numRows}条数据,该数据已经入库过！新数据有${carrierResultDataList.size}条")
//            }
//        }, {
//            log.error(it.printStackTrace())
//        })
    }

    /**
     * 移动-短信数据提取
     */
    private fun assistantSave(carrierResultDatas: CarrierResultData, obj: JsonObject) {

        carrierResultDatas.item = obj.getString("item")
        carrierResultDatas.result = obj.getString("result")
        // 预留字段
        carrierResultDatas.carrier_001 = ""
        carrierResultDatas.carrier_002 = ""

    }

}