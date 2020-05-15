package kavi.tech.service.service

import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.ext.mongo.FindOptions
import io.vertx.ext.sql.UpdateResult
import io.vertx.rxjava.ext.web.client.WebClient
import kavi.tech.service.common.extension.logger
import kavi.tech.service.common.extension.value
import kavi.tech.service.common.utils.CMCC
import kavi.tech.service.common.utils.CTCC
import kavi.tech.service.common.utils.CUCC
import kavi.tech.service.common.utils.Sha256Utils
import kavi.tech.service.mongo.model.*
import kavi.tech.service.mongo.schema.CarrierReportInfo
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
    val dataComboModel: DataComboModel,
    val dataUserInfoModel: DataUserInfoModel,
    val callLogDao: CallLogDao,
    val expenseCalendarDao: ExpenseCalendarDao,
    val internetInfoDao: InternetInfoDao,
    val paymentRecordDao: PaymentRecordDao,
    val smsInfoDao: SmsInfoDao,
    val userInfoDao: UserInfoDao,
    val comboDao: ComboDao,

    val callAnalysisService: CallAnalysisService,
    val contactsRegionService: ContactsRegionService,
    val friendSummaryService: FriendSummaryService,
    val userBehaviorService: UserBehaviorService,
    val userCallDetailsService: UserCallDetailsService,
    val userBasicService: UserBasicService,
    val cellPhoneService: CellPhoneService,
    val carrierReportInfoModel: CarrierReportInfoModel

) {
    @Autowired
    private lateinit var rxClient: WebClient
    private val log = logger(this::class)

    /**
     * 数据提取 根据传进来的task_id开始从mongo中读取数据 以及简单清洗后存入Mysql
     */
    fun beginDataByMongo(query: JsonObject, findOptions: FindOptions,userName: String,userIdCard: String): Single<List<UpdateResult>> {

        log.info("----------------- beginDataByMongo -----------------query ：${query}")

        // 读取mongo 数据库通话记录 并过滤数据插入Mysql
        val callLogResult = dataCallLogModel.queryListAndSave2Mysql(query)
            .flatMap {
                log.info("======:$it")
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
            .flatMap {
                smsInfoDao.insertBybatch(filterSmsInfo(it))

            }

        // 读取mongo 基础用户信息 并过滤数据插入mysql
        val userInfoResult = dataUserInfoModel.queryListAndSave2Mysql(query)
           .flatMap { userInfoDao.insert(filterUserInfo(it, userName, userIdCard)) }

       // 读取mongo 套餐数据 并过滤数据插入mysql
       val comboResult = dataComboModel.queryListAndSave2Mysql(query)
           .flatMap {
               val filterComboList = filterCombo(it)
               comboDao.insertBybatch(filterComboList)

           }
        return Single.concat(
            callLogResult,
            expenseCalendarResult,
            internetInfoResult,
            paymentRecordResult,
            smsInfoResult,
            userInfoResult,
            comboResult
        )
            .toList().toSingle()


    }


    /**
     * 通话记录过滤数据
     */
    private fun filterCallLog(list: List<JsonObject>): List<CallLog> {
        println("通话过滤：$list")

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
                    "CTCC"->{
                        println("++++++++++ CTCC ++++++++:$dataOut")
                        //todo 电信通话记录
                        val dataArray = dataOut.value<JsonArray>("data")
                        println("+++++===============： $dataArray")
                        val _list = dataArray?.mapNotNull { _any ->
                            try {
                                (_any as JsonObject)
                            } catch (e: Exception) {
                                null
                            }
                        }
                        _list?.forEach {listJson ->
                            println("========== CTCC ===========: $listJson")
                            listCallLog.add(CTCC.buildCallLog(listJson, mobile, taskId, billMonth))

                        }
                    }
                    else -> {
                        log.info("================ something must be error ====================")
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
                    println("================ ctcc ====: $it")
                    when (_operator) {
                        "CMCC" -> {
                            listExpenseCalendar.add(
                                CMCC.buildExpenseCalendar(it, mobile, taskId)
                            )
                        }
                        "CUCC" -> {
                            listExpenseCalendar.add(
                                CUCC.buildExpenseCalendar(it, mobile, taskId)
                            )
                        }
                        "CTCC" -> {
                            listExpenseCalendar.add(
                                CTCC.buildExpenseCalendar(it, mobile, taskId)
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
                    "CTCC" -> {
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
                                CTCC.buildInternetInfo(listJson, mobile, taskId, billMonth)
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
        println("充值缴费过滤：$list")
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
                    "CTCC" -> {
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
                                CTCC.buildPaymentRecord(listJson, mobile, taskId, billMonth)
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
        println("短信 filterSmsInfo========：$list")
        val listSmsInfo = mutableListOf<SmsInfo>()
        list.mapNotNull { json ->
            val dataOut = json.value<JsonObject>("data")
            println("data  =====：$dataOut")
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
                    "CTCC" -> {
                        println("===============sms ctcc=========")
                        val dataArray = dataOut.value<JsonArray>("data")
                        println("===========dataArray ：$dataArray")
                        val _list = dataArray?.mapNotNull { _any ->
                            try {
                                _any as JsonObject
                            } catch (e: Exception) {
                                null
                            }
                        }
                        _list?.map { listJson ->
                            listSmsInfo.add(
                                CTCC.buildSmsInfo(listJson, mobile, taskId, billMonth)
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
     * @param list mongo数据库查询出来的数据
     * @param name 外部传进来的用户姓名
     * @param idCard 外部传进来的用户身份证号
     */
    private fun filterUserInfo(list: List<JsonObject>, userName: String, userIdCard: String): UserInfo {
        println("filterUserInfo :$list")
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
                        userInfo =
                            CMCC.buileUserInfo(dataObj, mobile, taskId, billMonth, operator, userName, userIdCard)
                    }
                    "CUCC" -> {
                        userInfo =
                            CUCC.buileUserInfo(dataObj, mobile, taskId, billMonth, operator, userName, userIdCard)
                    }
                    "CTCC" -> {
                        userInfo =
                            CTCC.buildUserInfo(dataObj, mobile, taskId, billMonth, operator, userName, userIdCard)
                    }
                    else -> null
                }

            }
        }
        return userInfo
    }

    /**
     * 套餐记录过滤数据
     */
    private fun filterCombo(list: List<JsonObject>): List<Combo> {
        println("套餐 filterCombo========：${list.toString()}")
        val listCombo = mutableListOf<Combo>()
        list.mapNotNull { json ->
            println("具体套餐数据：$json")
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
                        val _list = dataOut.value<JsonArray>("data")?.mapNotNull { _any ->
                            try {
                                _any as JsonObject
                            } catch (e: Exception) {
                                null
                            }
                        }
                        _list?.map { arrList ->
                            when {
                                arrList.value<String>("type") == "1" -> {
                                    val _arrList = arrList.value<JsonArray>("arr")?.mapNotNull { _any ->
                                        try {
                                            _any as JsonObject
                                        } catch (e: Exception) {
                                            null
                                        }
                                    }
                                    _arrList?.map { _arr ->
                                        val resInfos = _arr.value<JsonArray>("resInfos")?.mapNotNull { _any ->
                                            try {
                                                _any as JsonObject
                                            } catch (e: Exception) {
                                                null
                                            }
                                        }

                                        resInfos?.map { _sec ->
                                            val _secResInfos =
                                                _sec.value<JsonArray>("secResInfos")?.mapNotNull { _sec ->
                                                    try {
                                                        _sec as JsonObject
                                                    } catch (e: Exception) {
                                                        null
                                                    }
                                                }
                                            _secResInfos?.map { secResInfos ->

                                                println("------------套餐数据-------------$secResInfos" )
                                                listCombo.add(
                                                    CMCC.buildCombo(secResInfos, mobile, taskId, billMonth)
                                                )


                                            }


                                        }


                                    }

                                }
                                else -> {

                                }
                            }

                        }
                    }
                    "CUCC" -> {
                        val dataArray = dataOut?.getJsonArray("fourpackage")
                        val _list = dataArray?.mapNotNull { _any ->
                            try {
                                println("==========fourpackage cucc==========:" + _any.toString())
                                _any as JsonObject
                            } catch (e: Exception) {
                                null
                            }
                        }
                        _list?.map { listJson ->
                            listCombo.add(
                                CUCC.buildCombo(listJson, mobile, taskId, billMonth)
                            )
                        }
                    }
                    "CTCC" -> {
                        val dataArray = dataOut?.getJsonArray("data")
                        val _list = dataArray?.mapNotNull { _any ->
                            try {
                                println("==========fourpackage cucc==========:" + _any.toString())
                                _any as JsonObject
                            } catch (e: Exception) {
                                null
                            }
                        }
                        _list?.map { listJson ->
                            listCombo.add(
                                CTCC.buildCombo(listJson, mobile, taskId, billMonth)
                            )
                        }
                    }
                    else -> null
                }

            }
        }
        return listCombo
    }


    /**
     * 数据清洗服务调用
     */
    fun dataClear(mobile: String, task_id: String): Single<JsonObject> {

        return Observable.concat(
            listOf(
                // 通话风险分析
                callAnalysisService.toCleaningCircleFriendsData(mobile, task_id).toObservable(),
                // 联系人区域汇总(近3（0-90天）／6月（0-180天）联系次数降序)
                contactsRegionService.getContactRegion(mobile, task_id).toObservable(),
                // 用户行为分析（按月汇总,汇总近6个月的）
                userBehaviorService.getCellBehavior(mobile, task_id).toObservable(),
                // 朋友圈联系人数量
                friendSummaryService.toCleaningCircleFriendsData(mobile, task_id).toObservable(),
                // 通话详单（近6月）
                userCallDetailsService.getUserDetails(mobile, task_id),
                friendSummaryService.getPeerNumTopList(mobile, task_id),
                friendSummaryService.getLocationTopList(mobile, task_id),
                userBasicService.getUserBasicInfo(mobile, task_id),
                cellPhoneService.getCellPhoneInfo(mobile, task_id)


            )
        ).toList().toSingle().map {


            val jsonObject = JsonObject()
            // 通话风险分析
            jsonObject.put("call_risk_analysis", it[0])
            // 联系人区域汇总(近3（0-90天）／6月（0-180天）联系次数降序)
            jsonObject.put("contact_region", it[1])
            // 用户行为分析（按月汇总,汇总近6个月的）
            jsonObject.put("cell_behavior", it[2])
            // 朋友圈联系人数量
            val friend_circleJsonObject = JsonObject()
            friend_circleJsonObject.put("summary", it[3])
            friend_circleJsonObject.put("peer_num_top_list", it[5])
            friend_circleJsonObject.put("location_top_list", it[6])
            jsonObject.put("friend_circle", friend_circleJsonObject)
            // 通话详单（近6月）
            jsonObject.put("call_contact_detail", it[4])
            jsonObject.put("user_basic", it[7])
            jsonObject.put("cell_phone", it[8])

        }

    }


    /**
     * 加签方法
     */
    fun sign(
        data: String,
        task_id: String,
        mobile: String,
        return_code: String,
        message: String,
        operation_time: String,
        nonce: String
    ): String {
        val arr = arrayOf(data, task_id, mobile, return_code, message, operation_time, nonce, NoticeRecords.KEY)
        val sb = StringBuffer()
        arr.forEach { item ->
            sb.append(item)
        }
        val str = sb.toString()
        return Sha256Utils.sha256(str)
    }

    /**
     * 运营商原始数据/运营商报告 结果封装
     * @param data 运营商原始数据 or 运营商报告
     * @param return_code  状态码
     * @param message 提示信息
     * @param operation_time 操作时间
     * @param task_id 任务ID
     * @param task_id 任务ID
     *
     */
    fun resultPacket(
        data: JsonObject,
        return_code: String,
        message: String,
        operation_time: Long,
        task_id: String,
        mobile: String,
        nonce: String
    ): JsonObject {
        // 推送数据结果封装
        val resultSend = JsonObject()
        val return_code = "00000"
        val message = "成功"
        val operation_time = System.currentTimeMillis()
        // 推送前加密  data，task_id ，mobile ，return_code ，message ，operation_time ，nonce，key,这样的顺序加签
        val sign = sign(data.toString(), task_id, mobile, return_code, message, operation_time.toString(), nonce)
        println("sign:$sign")
        return resultSend.put("data", data)
            .put("mobile", mobile)
            .put("task_id", task_id)
            .put("nonce", nonce)
            .put("return_code", return_code)
            .put("message", message)
            .put("operation_time", operation_time)
            .put("sign", sign)
    }


    /**
     * 存储运营商报告 or 原始数据  到Mongo 库
     * @param idCard 身份证号
     * @param mobile  手机号码
     * @param task_id  任务ID
     * @param name 姓名
     * @param item  类型： raw： 原始数据  report: 运营商报告
     * @param resultSend  具体存储的数据
     *
     */
    fun saveRecord(
        item: String,
        task_id: String,
        mobile: String,
        idCard: String,
        name: String,
        nonce: String,
        resultSend: JsonObject
    ) {
        println("==============saveReportToMongo =====================")
        val carrierReportInfo = CarrierReportInfo()
        carrierReportInfo.idCard = idCard
        carrierReportInfo.mobile = mobile
        carrierReportInfo.task_id = task_id
        carrierReportInfo.name = name
        carrierReportInfo.nonce = nonce
        carrierReportInfo.item = item
        carrierReportInfo.result = resultSend

        carrierReportInfoModel.insertReportDataIntoMongo(carrierReportInfo).subscribe({
            println("mobile: $mobile ,任务ID：$task_id ,$item 数据 insert to mongoDB success!")
        }, { it.printStackTrace() })
    }


}

