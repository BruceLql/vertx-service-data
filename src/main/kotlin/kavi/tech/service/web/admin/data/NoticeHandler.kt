package kavi.tech.service.web.admin.data

import io.vertx.core.http.HttpMethod
import io.vertx.core.json.JsonObject
import io.vertx.ext.mongo.FindOptions
import io.vertx.ext.sql.UpdateResult
import io.vertx.ext.web.RoutingContext
import io.vertx.rxjava.ext.web.client.WebClient
import kavi.tech.service.common.extension.GZIPUtils
import kavi.tech.service.common.extension.logger
import kavi.tech.service.common.extension.regexPhone
import kavi.tech.service.common.extension.value
import kavi.tech.service.mongo.model.*
import kavi.tech.service.mysql.dao.*
import kavi.tech.service.mysql.entity.NoticeRecords
import kavi.tech.service.service.CallAnalysisService
import kavi.tech.service.service.ContactsRegionService
import kavi.tech.service.service.FriendSummaryService
import kavi.tech.service.service.UserBehaviorService
import org.springframework.beans.factory.annotation.Autowired
import rx.Observable
import rx.Single
import tech.kavi.vs.web.ControllerHandler
import tech.kavi.vs.web.HandlerRequest

@HandlerRequest(path = "/notice", method = HttpMethod.POST)
class NoticeHandler @Autowired constructor(
    private val callLogDao: CallLogDao,
    private val userInfoDao: UserInfoDao,
    private val smsInfoDao: SmsInfoDao,
    private val internetInfoDao: InternetInfoDao,
    private val paymentRecordDao: PaymentRecordDao,
    private val expenseCalendarDao: ExpenseCalendarDao,
    val dataAccountInfoDao: DataAccountInfoModel,
    val dataCallLogDao: DataCallLogModel,
    val dataAppreciationInfoDao: DataAppreciationInfoModel,
    val dataCollectionInfoDao: DataCollectionInfoModel,
    val dataComboDao: DataComboModel,
    val dataExpenseCalendarDao: DataExpenseCalendarModel,
    val dataInternetInfoDao: DataInternetInfoModel,
    val dataOtherInfoDao: DataOtherInfoModel,
    val dataPaymentRecordDao: DataPaymentRecordModel,
    val dataSmsInfoDao: DataSmsInfoModel,
    val dataUserInfoDao: DataUserInfoModel,
    val noticeRecordsDao: NoticeRecordsDao,
    val callAnalysisService: CallAnalysisService,
    val contactsRegionService: ContactsRegionService,
    val friendSummaryService: FriendSummaryService,
    val userBehaviorService: UserBehaviorService


) : ControllerHandler() {
    private val log = logger(this::class)

    @Autowired
    private lateinit var rxClient: WebClient

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
        // 推送数据结果
        val resultSend = JsonObject()
        // notice 通知记录
        val noticeRecords = NoticeRecords()
        try {
            val params: JsonObject = event.bodyAsJson
            println(params.toString())
            if (params.isEmpty) {
                throw IllegalArgumentException("传入参数不合法！")
            }
            /* 查询条件 */
            val query = JsonObject()
            val mobile = params.value<String>("mobile") ?: throw IllegalArgumentException("缺少手机号码！")
            val task_id = params.value<String>("task_id") ?: throw IllegalArgumentException("缺少任务ID！")
            val back_url = params.value<String>("back_url") ?: throw IllegalArgumentException("缺少回调地址！")
            if (!regexPhone(mobile)) {
                throw IllegalArgumentException("(手机号)参数不合法！")
            }
            if (task_id.isEmpty()) {
                throw IllegalArgumentException("(任务ID)参数不合法！")
            }
            if (back_url.isEmpty()) {
                throw IllegalArgumentException("(回调地址)参数不合法！")
            }

            noticeRecords.back_url = back_url
            noticeRecords.mobile = mobile
            noticeRecords.task_id = task_id
            // 插入数据 返回 主键id  后续更新
            val noticeRecord = noticeRecordsDao.insert(noticeRecords).subscribe({ it }, { it.printStackTrace() })
            println("noticeRecord:$noticeRecord")

            val resultJsonObject = JsonObject()  // 存取最终返回结果

            query.put("mobile", mobile).put("mid", task_id)

            //   数据提取 根据传进来的task_id开始从mongo中读取数据 以及简单清洗后存入Mysql
            dataBegin(query, FindOptions()).flatMap {

                //   调用数据清洗服务 结果封装到 result
                dataClear(mobile, task_id)
            }.subscribe({

                resultSend.put("data", it)
                    .put("mobile", mobile)
                    .put("task_id", task_id)
                    .put("return_code", "00000")
                    .put("message", "成功")
                    .put("operation_time", System.currentTimeMillis())
                // TODO  数据推送服务  resultSend
                println("推送前结果： $resultSend")
                println("推送前结果size： ${resultSend.toString().length}")
                var pushData = GZIPUtils().compress(resultSend.toString())
                println("推送地址 : $back_url")

                rxClient.putAbs(back_url).method(HttpMethod.POST)
                    .sendStream(Observable.just(io.vertx.rxjava.core.buffer.Buffer.buffer(pushData))) { it ->
                        if (it.succeeded()) {
                            val response = it.result()
                            println("Got HTTP response with status ${response.statusCode()}")
                        } else {
                            it.cause().printStackTrace()
                        }
                    }

                event.response().end(result.put("message", "notice success").toString())

            }, { it.printStackTrace() })


//            event.response().end(result.put("message", "notice success").toString())
        } catch (e: Exception) {
            e.printStackTrace()
            result.put("message", e.message ?: "异常，请联系管理员排查")
            event.response().setStatusCode(500).end(result.toString()) // 返回数据
        }

    }

    /**
     * 数据提取 根据传进来的task_id开始从mongo中读取数据 以及简单清洗后存入Mysql
     */
    fun dataBegin(query: JsonObject, findOptions: FindOptions): Single<List<UpdateResult>> {

        return Observable.concat(
            listOf(
                // 3: 通话记录表
                dataCallLogDao.queryListAndSave2Mysql(query, findOptions).toObservable(),
                // 6: 消费记录表
                dataExpenseCalendarDao.queryListAndSave2Mysql(query, findOptions).toObservable(),
                // 7: 上网记录表
                dataInternetInfoDao.queryListAndSave2Mysql(query, findOptions).toObservable(),

                // 9: 交费记录表
                dataPaymentRecordDao.queryListAndSave2Mysql(query, findOptions).toObservable(),
                // 10:短信表
                dataSmsInfoDao.queryListAndSave2Mysql(query, findOptions).toObservable(),
                // 11:个人信息表
                dataUserInfoDao.queryListAndSave2Mysql(query, findOptions).toObservable()

            )
        ).toList().toSingle()

    }

    /**
     * 数据清洗服务调用
     */
    fun dataClear(mobile: String, task_id: String): Single<JsonObject> {

        return Observable.concat(
            listOf(
                callAnalysisService.toCleaningCircleFriendsData(mobile, task_id).toObservable(),
                contactsRegionService.getContactRegion(mobile, task_id).toObservable(),
                userBehaviorService.getCellBehavior(mobile, task_id).toObservable(),
                friendSummaryService.toCleaningCircleFriendsData(mobile, task_id)?.toObservable()

            )
        ).toList().toSingle().map {
            var jsonObject = JsonObject()
            jsonObject.put("call_risk_analysis", it[0])
            jsonObject.put("contact_region", it[1])
            jsonObject.put("cell_behavior", it[2])
            jsonObject.put("friend_circle", JsonObject().put("summary", it[3]))

        }

    }
}

