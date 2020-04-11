package kavi.tech.service.web.admin.record

import io.vertx.core.http.HttpMethod
import io.vertx.core.json.JsonObject
import io.vertx.ext.mongo.FindOptions
import io.vertx.ext.web.RoutingContext
import kavi.tech.service.common.extension.regexInt
import kavi.tech.service.common.extension.regexPhone
import kavi.tech.service.mongo.model.*
import kavi.tech.service.mysql.dao.*
import kavi.tech.service.mysql.entity.CallLog
import org.springframework.beans.factory.annotation.Autowired
import rx.Single
import tech.kavi.vs.web.ControllerHandler
import tech.kavi.vs.web.HandlerRequest
import java.lang.AssertionError

@HandlerRequest(path = "/listDetial", method = HttpMethod.POST)
class ListDetialHandler @Autowired constructor(

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
    private val callLogDao: CallLogDao,
    private val userInfoDao: UserInfoDao,
    private val smsInfoDao: SmsInfoDao,
    private val internetInfoDao: InternetInfoDao,
    private val paymentRecordDao: PaymentRecordDao
) : ControllerHandler() {


    override fun handle(event: RoutingContext) {

        // result 返回值
        val result = JsonObject()
            .put("message", "正常")
        try {
            val params: JsonObject = event.bodyAsJson
            println(params.toString())
            if (params.isEmpty) {
                throw IllegalArgumentException("传入参数不合法！")
            }
            // 查询条件
            val query = JsonObject()
            // 数据只展示 data
//            val findOptions = FindOptions().setFields(JsonObject().put("data", 1))
            val findOptions = FindOptions()


            val mobile = params.getString("mobile") ?: throw IllegalArgumentException("缺少手机号码！")
            val type = params.getInteger("type") ?: throw IllegalArgumentException("缺少type ！")
            // 手机号参数校验
            if (!regexPhone(mobile) || !regexInt(type.toString())) {
                throw IllegalArgumentException("参数不合法！")
            }
            query.put("mobile", mobile)
            // mid 参数校验
            val mid = params.getString("mid") ?: throw IllegalArgumentException("缺少mid !")
            query.put("mid", mid)
            println("=========入参：" + query.toString())

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
                println("=====================mongoData :" + it.toString())
                easyClearData(type, it)

                event.response().end(result.put("data", it).toString())
            }, {
                it.printStackTrace()
            })

            println("======list handle =========")

        } catch (e: Exception) {
            e.printStackTrace()
            result.put("message", e.message ?: "异常，请联系管理员排查")
            event.response().setStatusCode(500).end(result.toString()) // 返回数据
        }
    }

    /**
     *  数据从Mongo读取出来，简单提取后存入Mysql
     */
    fun easyClearData(type: Int, data: List<JsonObject>) {
        if (data.isEmpty()) { throw  IllegalAccessException("数据为空！") }

        when (type) {
            // 账户信息数据
            1 ->callLogDao.callLogDataInsert(data)
            // 通话记录数据
            3 ->callLogDao.callLogDataInsert(data)
            // 消费记录信息
            6 -> callLogDao.callLogDataInsert(data)
            // 上网详情信息
            7 -> internetInfoDao.internetInfoDataInsert(data)
            // 交费充值记录表
            9 -> paymentRecordDao.paymentRecordDataInsert(data)
            // 短信数据
            10 -> smsInfoDao.smsInfoDataInsert(data)
            // 个人信息数据
            11 -> userInfoDao.userInfoDataInsert(data)

        }


    }

}
