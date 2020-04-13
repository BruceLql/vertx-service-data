package kavi.tech.service.web.admin.data

import io.vertx.core.http.HttpMethod
import io.vertx.core.json.JsonObject
import io.vertx.ext.mongo.FindOptions
import io.vertx.ext.web.RoutingContext
import kavi.tech.service.common.extension.logger
import kavi.tech.service.common.extension.regexPhone
import kavi.tech.service.mongo.model.*
import kavi.tech.service.mysql.dao.*
import kavi.tech.service.mysql.entity.NoticeRecords
import org.springframework.beans.factory.annotation.Autowired
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
    val noticeRecordsDao: NoticeRecordsDao

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
            val mobile = params.getString("mobile") ?: throw IllegalArgumentException("缺少手机号码！")
            val task_id = params.getString("task_id") ?: throw IllegalArgumentException("缺少任务ID！")
            val back_url = params.getString("back_url") ?: throw IllegalArgumentException("缺少回调地址！")
            if (!regexPhone(mobile)) {
                throw IllegalArgumentException("(手机号)参数不合法！")
            }
            if (task_id.isEmpty()) {
                throw IllegalArgumentException("(任务ID)参数不合法！")
            }
            if (back_url.isEmpty()) {
                throw IllegalArgumentException("(回调地址)参数不合法！")
            }

            noticeRecords.back_url=back_url
            noticeRecords.mobile=mobile
            noticeRecords.task_id =task_id
            // 插入数据 返回 主键id  后续更新
            val noticeRecord = noticeRecordsDao.insert(noticeRecords).subscribe({ it }, { it.printStackTrace() })
            println("noticeRecord:$noticeRecord")

            val resultJsonObject = JsonObject()  // 存取最终返回结果

            query.put("mobile",mobile).put("task_id",task_id)

            // TODO  数据提取 根据传进来的task_id开始从mongo中读取数据
             dataClear(mobile,task_id,1)
             dataClear(mobile,task_id,3)
             dataClear(mobile,task_id,6)
             dataClear(mobile,task_id,7)
             dataClear(mobile,task_id,9)
             dataClear(mobile,task_id,10)
             dataClear(mobile,task_id,11)

            /*dataUserInfoDao.list(query, findOptions).subscribe({
                // 个人信息数据
                userInfoDao.userInfoDataInsert(it)
            },{it.printStackTrace()})*/


            // TODO  数据清洗
            // TODO  数据推送






            event.response().end(result.put("message", "notice success").toString())
        } catch (e: Exception) {
            e.printStackTrace()
            result.put("message", e.message ?: "异常，请联系管理员排查")
            event.response().setStatusCode(500).end(result.toString()) // 返回数据
        }

    }

    fun dataClear(mobile: String, task_id: String, type: Int) {

        try {
            // 查询条件
            val query = JsonObject()
            val findOptions = FindOptions()

            query.put("mobile", mobile)
            query.put("mid", task_id)


            when (type) {
                // 1：账户信息表
                1 -> dataAccountInfoDao.list(query, findOptions)
                // 2：增值业务表
                2 -> dataAppreciationInfoDao.list(query, findOptions)
                // 3: 通话记录表
                3 -> dataCallLogDao.list(query, findOptions)
                // 4: 代收费用表
                4 -> dataCollectionInfoDao.list(query, findOptions)
                // 5: 套餐表
                5 -> dataComboDao.list(query, findOptions)
                // 6: 消费记录表
                6 -> dataExpenseCalendarDao.list(query, findOptions)
                // 7: 上网记录表
                7 -> dataInternetInfoDao.list(query, findOptions)
                // 8: 其它信息表
                8 -> dataOtherInfoDao.list(query, findOptions)
                // 9: 交费记录表
                9 -> dataPaymentRecordDao.list(query, findOptions)
                // 10:短信表
                10 -> dataSmsInfoDao.list(query, findOptions)
                // 11:个人信息表
                11 -> dataUserInfoDao.list(query, findOptions)

                else -> Single.error(IllegalArgumentException("参数不合法！"))
            }.subscribe({
                println("========= $type ============mongoData :$it")
                easyClearData(type, it)
            }, {
                it.printStackTrace()
            })

        } catch (e: Exception) {
            e.printStackTrace()
        }
    }

    /**
     *  数据从Mongo读取出来，简单提取后存入Mysql
     */
    private fun easyClearData(type: Int, data: List<JsonObject>) {
        if (data.isEmpty()) {
            throw  IllegalAccessException("数据为空！")
        }

        when (type) {
            // 账户信息数据
//            1 -> callLogDao.callLogDataInsert(data)
            // 通话记录数据
            3 -> callLogDao.callLogDataInsert(data)
            // 消费记录信息
            6 -> expenseCalendarDao.expenseCalendarDataInsert(data)
            // 上网详情信息
            7 -> internetInfoDao.internetInfoDataInsert(data)
            // 交费充值记录表
            9 -> paymentRecordDao.paymentRecordDataInsert(data)
            // 短信数据
            10 -> smsInfoDao.smsInfoDataInsert(data)
            // 个人信息数据
            11 -> userInfoDao.userInfoDataInsert(data)

            else -> print("参数异常")
        }


    }

}
