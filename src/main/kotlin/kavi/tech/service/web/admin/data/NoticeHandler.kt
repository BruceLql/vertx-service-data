package kavi.tech.service.web.admin.data

import io.vertx.core.http.HttpMethod
import io.vertx.core.json.JsonObject
import io.vertx.ext.mongo.FindOptions
import io.vertx.ext.web.RoutingContext
import kavi.tech.service.common.extension.error
import kavi.tech.service.common.extension.logger
import kavi.tech.service.common.extension.regexPhone
import kavi.tech.service.common.extension.value
import kavi.tech.service.common.utils.OperatorCertificationComponent
import kavi.tech.service.mongo.schema.CarrierReportInfo
import kavi.tech.service.service.CarrierService
import kavi.tech.service.service.ReportService
import org.springframework.beans.factory.annotation.Autowired
import rx.Single
import tech.kavi.vs.web.ControllerHandler
import tech.kavi.vs.web.HandlerRequest

@HandlerRequest(path = "/notice", method = HttpMethod.POST)
class NoticeHandler @Autowired constructor(
    val reportService: ReportService,
    val carrierService: CarrierService,
    val operatorCertificationComponent: OperatorCertificationComponent

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
        val params: JsonObject = try {
            event.bodyAsJson
        } catch (e: Exception) {
            event.response().error(e) // 返回数据
            return
        }
        /* 查询条件 */
        val query = JsonObject()
        val mobile = params.value<String>("mobile")
        val taskId = params.value<String>("taskId")
        val backUrl = params.value<String>("backUrl")
        val nonce = params.value<String>("nonce")
        val name = params.value<String>("name")
        val idCard = params.value<String>("cid")
        val isCache = params.value<Boolean>("isCache")
        try {
            log.info("/data/notice 请求参数：${params.toString()}")
            if (params.isEmpty) {
                throw IllegalArgumentException("传入参数不合法！")
            }

            mobile ?: throw IllegalArgumentException("缺少手机号码！")
            taskId ?: throw IllegalArgumentException("缺少任务ID！")
            backUrl ?: throw IllegalArgumentException("缺少回调地址！")
            nonce ?: throw IllegalArgumentException("缺少nonce！")
            name ?: throw IllegalArgumentException("缺少name！")
            idCard ?: throw IllegalArgumentException("缺少idCard！")
            isCache ?: throw IllegalArgumentException("缺少isCache！")
            if (!regexPhone(mobile)) {
                throw IllegalArgumentException("(手机号)参数不合法！")
            }
            if (taskId.isEmpty()) {
                throw IllegalArgumentException("(任务ID)参数不合法！")
            }
            if (backUrl.isEmpty()) {
                throw IllegalArgumentException("(回调地址)参数不合法！")
            }
        } catch (e: Exception) {
            e.printStackTrace()
            result.put("message", e.message ?: "异常，请联系管理员排查")
            event.response().setStatusCode(500).error(e, result) // 返回数据
            return
        }
        query.put("mobile", mobile).put("mid", taskId)
        when (isCache) {
            false -> { // 没有缓存数据
                //   数据提取 根据传进来的task_id开始从mongo中读取数据 以及简单清洗后存入Mysql
                reportService.beginDataByMongo(query, FindOptions(), name, idCard).flatMap {
                    dataMapAndPush(taskId, mobile, idCard, name, nonce,backUrl,isCache)
                }
            }
            else -> { // 有缓存数据
                //  往爬虫记录添加一条成功数据
                val query = JsonObject()
                val fileds = JsonObject()
                query.put("mobile","18016875613").put("success",true).put("task_id",taskId)
                fileds.put("_id",0).put("mobile",1).put("operator",1).put("statistics",1).put("city",1).put("province",1).put("ip",1).put("app_name",1)
                reportService.cacheData(query,fileds).flatMap {

                    dataMapAndPush(taskId, mobile, idCard, name, nonce,backUrl,isCache)
                }

            }
        }.subscribe({
            event.response().end(result.put("message", "notice success").toString())
        }, {
            it.printStackTrace()
            event.response().end(result.put("message", "notice failed").toString())
        })
    }

    /**
     * 数据清洗封装成相应格式（运营商原始数据、运营商报告），以及推送报文到指定URL
     */
    fun dataMapAndPush(taskId: String, mobile: String, idCard: String, name: String, nonce: String, backUrl: String, isCache :Boolean): Single<CarrierReportInfo> {
        //   调用数据清洗服务 结果封装到 result
       return reportService.dataClear(mobile, taskId).flatMap {
            // todo 清洗服务失败的情况下  调整return_code message
            val return_code = "00000"
            val message = "成功"
            val operation_time = System.currentTimeMillis()
            // 封装运营商原始数据报文格式
            val resultSend =
                reportService.resultPacket(it, return_code, message, operation_time, taskId, mobile, nonce)
            // 存储运营商报告
            reportService.saveRecord("report", taskId, mobile, idCard, name, nonce, resultSend).flatMap {
                // 查询原始数据封装到result
                carrierService.dataRaw(mobile, taskId).flatMap {
                    // todo 原始数据获取失败的情况下  调整return_code message
                    val return_code = "00000"
                    val message = "成功"
                    val operation_time = System.currentTimeMillis()
                    // 封装运营商原始数据报文格式
                    val resultSend =
                        reportService.resultPacket(it, return_code, message, operation_time, taskId, mobile, nonce)
                    log.info("推送前结果： $resultSend")
                    log.info("推送前结果size： ${resultSend.toString().length}")
                    log.info("推送地址 : $backUrl")
                    operatorCertificationComponent.callBackPlatform(resultSend, backUrl, backUrl)
                    // 加签名后  运营商原始数据存储入库
                    reportService.saveRecord("raw", taskId, mobile, idCard, name, nonce, resultSend)
                }
            }
        }

    }
}



