package kavi.tech.service.service

import io.vertx.core.http.HttpMethod
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.ext.mongo.FindOptions
import io.vertx.ext.sql.UpdateResult
import io.vertx.rxjava.ext.web.client.WebClient
import kavi.tech.service.common.extension.GZIPUtils
import kavi.tech.service.common.extension.value
import kavi.tech.service.common.utils.CMCC
import kavi.tech.service.common.utils.CUCC
import kavi.tech.service.mongo.model.*
import kavi.tech.service.mysql.dao.*
import kavi.tech.service.mysql.entity.*
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import rx.Observable
import rx.Single

@Service
class ReportService @Autowired constructor(
    val dataCallLogModel: DataCallLogModel,
    val dataExpenseCalendarModel: DataExpenseCalendarModel,
    val dataInternetInfoMondel: DataInternetInfoModel,
    val dataPaymentRecordMondel: DataPaymentRecordModel,
    val dataSmsInfoModel: DataSmsInfoModel,
    val dataUserInfoModel: DataUserInfoModel,
    val callLogDao: CallLogDao,
    val expenseCalendarDao: ExpenseCalendarDao,
    val internetInfoDao: InternetInfoDao,
    val paymentRecordDao: PaymentRecordDao,
    val smsInfoDao: SmsInfoDao,
    val userInfoDao: UserInfoDao,

    val callAnalysisService: CallAnalysisService,
    val contactsRegionService: ContactsRegionService,
    val friendSummaryService: FriendSummaryService,
    val userBehaviorService: UserBehaviorService

) {
    @Autowired
    private lateinit var rxClient: WebClient

    fun report(mobile: String, taskId: String) {

    }

    /**
     * 数据提取 根据传进来的task_id开始从mongo中读取数据 以及简单清洗后存入Mysql
     */
    fun beginDataByMongo(query: JsonObject, findOptions: FindOptions): Single<List<UpdateResult>> {

        println("----------------- beginDataByMongo -----------------query ：${query.toString()}")

        // 读取mongo 数据库通话记录 并过滤数据插入Mysql
        val callLogResult = dataCallLogModel.queryListAndSave2Mysql(query)
            .flatMap {
                println("======:" + it)
                callLogDao.insertBybatch(filterCallLog(it))
            }

        // 读取mongo 消费记录 并过滤数据插入mysql
        val expenseCalendarResult = dataExpenseCalendarModel.queryListAndSave2Mysql(query)
            .flatMap { expenseCalendarDao.insertBybatch(filterExpenseCalendar(it)) }

        // 读取mongo 上网详情记录 并过滤数据插入mysql
        val internetInfoResult = dataInternetInfoMondel.queryListAndSave2Mysql(query)
            .flatMap { internetInfoDao.insertBybatch(filterInternetInfo(it)) }

        // 读取mongo 交费记录 并过滤数据插入mysql
        val paymentRecordResult = dataPaymentRecordMondel.queryListAndSave2Mysql(query)
            .flatMap { paymentRecordDao.insertBybatch(filterPaymentRecord(it)) }

        // 读取mongo 短信数据 并过滤数据插入mysql
        val smsInfoResult = dataSmsInfoModel.queryListAndSave2Mysql(query)
            .flatMap { smsInfoDao.insertBybatch(filterSmsInfo(it)) }

        // 读取mongo 基础用户信息 并过滤数据插入mysql
        val userInfooResult = dataUserInfoModel.queryListAndSave2Mysql(query)
            .flatMap { userInfoDao.insert(filterUserInfo(it)) }

        return Single.concat(
            callLogResult,
            expenseCalendarResult,
            internetInfoResult,
            paymentRecordResult,
            smsInfoResult,
            userInfooResult
        )
            .toList().toSingle()


    }


    /**
     * 通话记录过滤数据
     */
    private fun filterCallLog(list: List<JsonObject>): List<CallLog> {
        println("通话过滤：${list.toString()}")

        val listCallLog = mutableListOf<CallLog>()
        list.mapNotNull { json ->
            // dataOut 固定格式
            val dataOut = json.value<JsonObject>("data")

            val operator = json.value<String>("operator")
            val taskId = json.value<String>("mid")
            val mobile = json.value<String>("mobile")
            val billMonth = json.value<String>("bill_month")
            if (taskId == null || mobile == null) {
                return@mapNotNull null
            }

            operator?.let { _operator ->
                when (_operator) {
                    "CMCC" -> {
                        val dataArray = dataOut.value<JsonArray>("data")
                        val _list = dataArray?.mapNotNull { _any ->
                            try {
                                (_any as JsonObject)
                            } catch (e: Exception) {
                                null
                            }
                        }
                        _list?.forEach { listJson ->

                            listCallLog.add(CMCC.buildCallLog(listJson, mobile, taskId, billMonth))
                        }
                    }
                    "CUCC" -> {
                        //  pageMap  加了 !!  转换异常的话会报错
                        val dataArray = dataOut.value<JsonObject>("pageMap")!!.getJsonArray("result")
                        val _list = dataArray?.mapNotNull { _any ->
                            try {
                                (_any as JsonObject)
                            } catch (e: Exception) {
                                null
                            }
                        }
                        _list?.forEach { listJson ->

                            listCallLog.add(CUCC.buildCallLog(listJson, mobile, taskId, billMonth))
                        }
                    }
                    else -> {

                    }

                }
            }
        }
        return listCallLog
    }


    /**
     * 消费记录过滤数据
     */
    private fun filterExpenseCalendar(list: List<JsonObject>): List<ExpenseCalendar> {
        println("消费记录过滤：${list.toString()}")
        val listExpenseCalendar = mutableListOf<ExpenseCalendar>()
        list.mapNotNull { json ->
            val dataArray = json.value<JsonArray>("data")
            val operator = json.value<String>("operator")
            val taskId = json.value<String>("mid")
            val mobile = json.value<String>("mobile")
            val billMonth = json.value<String>("bill_month")
            if (taskId == null || mobile == null) {
                return@mapNotNull null
            }
            val _list = dataArray?.mapNotNull { _any ->
                try {
                    _any as JsonObject
                } catch (e: Exception) {
                    null
                }
            }

            operator?.let { _operator ->
                _list?.map { it ->
                    when (_operator) {
                        "CMCC" -> {
                            listExpenseCalendar.add(
                                CMCC.buildExpenseCalendar(it, mobile, taskId, billMonth)
                            )
                        }
                        "CUCC" -> {
                            listExpenseCalendar.add(
                                CUCC.buildExpenseCalendar(it, mobile, taskId)
                            )
                        }
                        else -> null
                    }
                }
            }

        }
        return listExpenseCalendar

    }


    /**
     * 上网流量详情过滤数据
     */
    private fun filterInternetInfo(list: List<JsonObject>): List<InternetInfo> {

        println("上网流量过滤：${list}")
        val listInternetInfo = mutableListOf<InternetInfo>()
        list.mapNotNull { json ->
            val dataOut = json.value<JsonObject>("data")

            val operator = json.value<String>("operator")
            val taskId = json.value<String>("mid")
            val mobile = json.value<String>("mobile")
            val billMonth = json.value<String>("bill_month")
            if (taskId == null || mobile == null) {
                return@mapNotNull null
            }

            operator?.let { _operator ->

                when (_operator) {
                    "CMCC" -> {
                        val dataArray = dataOut.value<JsonArray>("data")
                        val _list = dataArray?.mapNotNull { _any ->
                            try {
                                _any as JsonObject
                            } catch (e: Exception) {
                                null
                            }
                        }
                        _list?.map { listJson ->
                            listInternetInfo.add(
                                CMCC.buildInternetInfo(listJson, mobile, taskId, billMonth)
                            )
                        }
                    }
                    "CUCC" -> {
                        val dataArray = dataOut.value<JsonArray>("pagelist")
                        val _list = dataArray?.mapNotNull { _any ->
                            try {
                                _any as JsonObject
                            } catch (e: Exception) {
                                null
                            }
                        }
                        _list?.map { listJson ->
                            listInternetInfo.add(
                                CUCC.buildInternetInfo(listJson, mobile, taskId, billMonth)
                            )
                        }
                    }
                    else -> null
                }
            }
        }
        return listInternetInfo

    }

    /**
     * 充值缴费记录过滤数据
     */
    private fun filterPaymentRecord(list: List<JsonObject>): List<PaymentRecord> {
        println("充值缴费过滤：${list.toString()}")
        val listPaymentRecord = mutableListOf<PaymentRecord>()
        list.mapNotNull { json ->
            val dataOut = json.value<JsonObject>("data")

            val operator = json.value<String>("operator")
            val taskId = json.value<String>("mid")
            val mobile = json.value<String>("mobile")
            val billMonth = json.value<String>("bill_month")
            if (taskId == null || mobile == null) {
                return@mapNotNull null
            }


            operator?.let { _operator ->

                when (_operator) {
                    "CMCC" -> {
                        val dataArray = dataOut.value<JsonArray>("data")
                        val _list = dataArray?.mapNotNull { _any ->
                            try {
                                _any as JsonObject
                            } catch (e: Exception) {
                                null
                            }
                        }
                        _list?.map { listJson ->

                            listPaymentRecord.add(
                                CMCC.buildPaymentRecord(listJson, mobile, taskId, billMonth)
                            )
                        }
                    }
                    "CUCC" -> {
                        val dataArray = dataOut.value<JsonArray>("totalResult")
                        val _list = dataArray?.mapNotNull { _any ->
                            try {
                                _any as JsonObject
                            } catch (e: Exception) {
                                null
                            }
                        }
                        _list?.map { listJson ->
                            listPaymentRecord.add(
                                CUCC.buildPaymentRecord(listJson, mobile, taskId, billMonth)
                            )
                        }
                    }
                    else -> null
                }

            }
        }
        return listPaymentRecord

    }

    /**
     * 短信记录过滤数据
     */
    private fun filterSmsInfo(list: List<JsonObject>): List<SmsInfo> {
        println("短信 filterSmsInfo========：${list.toString()}")
        val listSmsInfo = mutableListOf<SmsInfo>()
        list.mapNotNull { json ->
            val dataOut = json.value<JsonObject>("data")

            val operator = json.value<String>("operator")
            val taskId = json.value<String>("mid")
            val mobile = json.value<String>("mobile")
            val billMonth = json.value<String>("bill_month")
            if (taskId == null || mobile == null) {
                return@mapNotNull null
            }

            operator?.let { _operator ->

                when (_operator) {
                    "CMCC" -> {
                        val dataArray = dataOut.value<JsonArray>("data")
                        val _list = dataArray?.mapNotNull { _any ->
                            try {
                                _any as JsonObject
                            } catch (e: Exception) {
                                null
                            }
                        }
                        _list?.map { listJson ->
                            listSmsInfo.add(
                                CMCC.buileSmsInfo(listJson, mobile, taskId, billMonth)
                            )
                        }
                    }
                    "CUCC" -> {
                        val dataArray = dataOut.value<JsonObject>("pageMap")!!.getJsonArray("result")
                        val _list = dataArray?.mapNotNull { _any ->
                            try {
                                _any as JsonObject
                            } catch (e: Exception) {
                                null
                            }
                        }
                        _list?.map { listJson ->
                            listSmsInfo.add(
                                CUCC.buileSmsInfo(listJson, mobile, taskId, billMonth)
                            )
                        }
                    }
                    else -> null
                }

            }
        }
        return listSmsInfo
    }

    /**
     * 基础用户信息过滤数据
     */
    private fun filterUserInfo(list: List<JsonObject>): UserInfo {
        println("filterUserInfo :" + list)
        var userInfo = UserInfo()
        list.mapNotNull { json ->
            val dataObj = json.value<JsonObject>("data")!!
            val operator = json.value<String>("operator")
            val taskId = json.value<String>("mid")
            val mobile = json.value<String>("mobile")
            val billMonth = json.value<String>("bill_month")
            if (taskId == null || mobile == null) {
                return@mapNotNull null
            }
            operator?.let { _operator ->
                when (_operator) {
                    "CMCC" -> {
                        userInfo = CMCC.buileUserInfo(dataObj, mobile, taskId, billMonth)
                    }
                    "CUCC" -> {
                        userInfo = CUCC.buileUserInfo(dataObj, mobile, taskId, billMonth)
                    }
                    else -> null
                }

            }
        }
        return userInfo
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
                friendSummaryService.toCleaningCircleFriendsData(mobile, task_id).toObservable()

            )
        ).toList().toSingle().map {


            var jsonObject = JsonObject()
            jsonObject.put("call_risk_analysis", it[0])
            jsonObject.put("contact_region", it[1])
            jsonObject.put("cell_behavior", it[2])
            jsonObject.put("friend_circle", JsonObject().put("summary", it[3]))

        }

    }

    /**
     * @param backUrl 回调地址
     * @author max
     * @param resultSend 运营商原始数据 json 格式
     */
    fun pushData(backUrl: String, resultSend: JsonObject) {
        val pushData = GZIPUtils().compress(resultSend.toString())

        rxClient.putAbs(backUrl).method(HttpMethod.POST)
            .sendStream(Observable.just(io.vertx.rxjava.core.buffer.Buffer.buffer(pushData))) { it ->
                if (it.succeeded()) {
                    val response = it.result()
                    println("Got HTTP response with status ${response.statusCode()}")
                } else {
                    it.cause().printStackTrace()
                    // todo 失败重新推送
                }
            }
    }


}

