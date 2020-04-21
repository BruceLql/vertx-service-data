package kavi.tech.service.web.admin.data

import io.vertx.core.http.HttpMethod
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.RoutingContext
import kavi.tech.service.common.extension.logger
import kavi.tech.service.common.extension.regexPhone
import kavi.tech.service.mysql.component.SQL
import kavi.tech.service.mysql.dao.QueryRecordsDao
import kavi.tech.service.mysql.dao.ResultDataDao
import kavi.tech.service.mysql.entity.QueryRecords
import kavi.tech.service.mysql.entity.ResultData
import org.springframework.beans.factory.annotation.Autowired
import rx.Single
import tech.kavi.vs.web.ControllerHandler
import tech.kavi.vs.web.HandlerRequest

/**
 * @param mobile 必传参数 手机号
 *
 */
@HandlerRequest(path = "/query", method = HttpMethod.POST)
class QueryHandler @Autowired constructor(
    private val queryRecordsDao: QueryRecordsDao,
    private val resultDataDao: ResultDataDao
) : ControllerHandler() {
    private val log = logger(this::class)
    /**
     * 爬虫程序执行完毕后通知
     * @author max
     * @param mobile  手机号
     * @param task_id  任务ID
     * @param back_url 回调地址
     * */
    override fun handle(event: RoutingContext) {
        log.info("=========/data/notice==============")
        // result 返回值
        val result = JsonObject()
        val data = JsonObject()
        result.put("status", "0")
        result.put("message", "success")

        try {
            val params: JsonObject = event.bodyAsJson
            println(params.toString())
            if (params.isEmpty) {
                throw IllegalArgumentException("传入参数不合法！")
            }
            val mobile = params.getString("mobile") ?: throw IllegalArgumentException("缺少手机号码！")
            val task_id = params.getString("task_id")
            val item = params.getString("item")  // ”raw“; "report"
            if (!regexPhone(mobile)) {
                throw IllegalArgumentException("(手机号)参数不合法！")
            }
            val queryRecord = QueryRecords()
            queryRecord.mobile = mobile
            if (task_id.isNotEmpty()) {
                queryRecord.task_id = task_id
            }
            // 获取数据的方式 1:推送 2：主动查询
            queryRecord.type = 2
            // 状态 0：success 1:error
            queryRecord.status = 0
            // 保存请求记录
//            queryRecordsDao.insert(queryRecord).flatMap {
            queryLastestTaskId(queryRecord).flatMap {
                // 返回手机号和 任务ID
                result.put("mobile", mobile)
                result.put("task_id", it.getString("task_id"))
                val listOf = if (item.isNotEmpty()) {
                    listOf(Pair("task_id", it.getString("task_id")), Pair("mobile", mobile), Pair("item", item))
                } else {
                    listOf(Pair("task_id", it.getString("task_id")), Pair("mobile", mobile))
                }


                resultDataDao.selectData(listOf)

            }.subscribe({
                println(it.toString())
                val ss = it.getString("result")

                result.put("data", JsonObject(ss))
                //  数据返回
                event.response().end(result.toString())

            }, { it.printStackTrace() })


        } catch (e: Exception) {
            e.printStackTrace()
            result.put("message", e.message ?: "异常，请联系管理员排查")
            event.response().setStatusCode(500).end(result.toString()) // 返回数据
        }
    }

    /**
     * 查询最近一次的采集结果的任务ID
     */
    fun queryLastestTaskId(queryRecord: QueryRecords): Single<JsonObject> {

        val sql = SQL.init {
            SELECT("task_id")
            FROM(ResultData.tableName)
            WHERE(Pair("mobile", queryRecord.mobile))
            ORDER_BY("created_at")
        } + " DESC"
        println("queryLastestTaskId:$sql")
        return queryRecordsDao.one(sql)
    }

}

