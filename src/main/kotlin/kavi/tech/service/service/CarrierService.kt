package kavi.tech.service.service

import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import kavi.tech.service.common.extension.value
import kavi.tech.service.mysql.dao.*
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import rx.Observable
import rx.Single

@Service
class CarrierService @Autowired constructor(
    val callLogDao: CallLogDao,
    val expenseCalendarDao: ExpenseCalendarDao,
    val internetInfoDao: InternetInfoDao,
    val paymentRecordDao: PaymentRecordDao,
    val smsInfoDao: SmsInfoDao,
    val userInfoDao: UserInfoDao,
    val comboDao: ComboDao
) {

    /**
     * 原始数据封装
     */
    fun dataRaw(mobile: String, task_id: String): Single<JsonObject> {

        return Observable.concat(
            listOf(
                findCallLog(mobile, task_id), // 0
                findSmss(mobile, task_id), // 1
                findRecharges(mobile, task_id), // 2
                findNets(mobile, task_id),
                findUserInfo(mobile, task_id),
                findExpenseCalendar(mobile, task_id),
                findCombo(mobile, task_id)
            )
        ).toList().toSingle().map {

            val callLog = it[0]
            val resultJson = JsonObject()

            // calls 通话记录原始数据  ==========================================================
            val calls = callsInto(callLog[0], callLog[1])
            resultJson.put("calls", calls)
            // smses 短信记录原始数据  ==========================================================
            val smsInfoDao = it[1]
            val smses = smsesInto(smsInfoDao[0], smsInfoDao[1])
            resultJson.put("smses", smses)
            // recharges 充值记录原始数据  ==========================================================
            val rechargesList = it[2]
            println("recharge_time:$rechargesList")
            val recharges = rechargesInto(rechargesList[0])
            resultJson.put("recharges", recharges)
            // nets 上网记录原始数据  ==========================================================
            val netsList = it[3]
            println("netsList:$netsList")
            val nets = netsInto(netsList[0], netsList[1])
            resultJson.put("nets", nets)
            // carrier_user_info 用户基本信息 原始数据  ==========================================================
            val userInfoList = it[4]
            println("netsList:$userInfoList")
            val carrier_user_info = userInfoInto(userInfoList[0])
            resultJson.put("carrier_user_info", carrier_user_info)
            // bills 月账单 原始数据  ==========================================================
            val billsList = it[5]
            println("billsList:$billsList")
            val bills = billsInto(billsList[0])
            resultJson.put("bills", bills)

            // 套餐数据 原始数据  ==========================================================
            val packagesList = it[6]
            println("packagesList:$packagesList")
            val packages = packagesInto(packagesList[0], packagesList[1])
            resultJson.put("packages", packages)
            // 亲情号码数据 联通没有 先模拟 =================================================
            val families = ArrayList<JsonObject>()
            val familieJsonObject = JsonObject()
            familieJsonObject.put("family_num", "").put("items", ArrayList<JsonObject>())
            families.add(familieJsonObject)
            resultJson.put("families", families)

        }

    }

    fun callsInto(callsData: List<JsonObject>, callsCount: List<JsonObject>): ArrayList<JsonObject> {
        val calls = ArrayList<JsonObject>()

        (0..callsData.size - 1).map { d ->
            val jsonObject = JsonObject()
            val data = callsCount[d].value<JsonObject>("data")
            val bill_month = data?.value<String>("bill_month")
            val total_size = data?.getInteger("total_size")
            jsonObject.put("bill_month", bill_month)
            jsonObject.put("total_size", total_size)
            val items = callsData[d].value<JsonArray>("data")

            jsonObject.put("items", items)
            calls.add(jsonObject)
        }
        return calls
    }


    fun smsesInto(smsesList: List<JsonObject>, callsCount: List<JsonObject>): ArrayList<JsonObject> {
        val smses = ArrayList<JsonObject>()

        (0..smsesList.size - 1).map { d ->
            val jsonObject = JsonObject()
            val data = callsCount[d].value<JsonObject>("data")
            val bill_month = data?.value<String>("bill_month")
            val total_size = data?.getInteger("total_size")
            jsonObject.put("bill_month", bill_month)
            jsonObject.put("total_size", total_size)
            val items = smsesList[d].value<JsonArray>("data")?:JsonArray()

            jsonObject.put("items", items)
            smses.add(jsonObject)
        }
        return smses
    }

    fun rechargesInto(rechargesList: List<JsonObject>): ArrayList<JsonObject> {
        val recharges = ArrayList<JsonObject>()

        rechargesList.mapNotNull { json ->
            println("json-----:$json")
            val dataValue = json.value<JsonArray>("data")

            dataValue?.mapNotNull {
                it as JsonObject
                val jsonObject = JsonObject()

                jsonObject.put("amount", it.value<Int>("amount"))
                jsonObject.put("recharge_time", it.value<String>("recharge_time"))
                jsonObject.put("type", it.value<String>("type"))
                recharges.add(jsonObject)
            }

        }
        return recharges
    }


    fun netsInto(netsData: List<JsonObject>, netsCount: List<JsonObject>): ArrayList<JsonObject> {

        val nets = ArrayList<JsonObject>()

        (0..netsData.size - 1).map { d ->
            val jsonObject = JsonObject()
            val data = netsCount[d].value<JsonObject>("data")
            val bill_month = data?.value<String>("bill_month")
            val total_size = data?.getInteger("total_size")
            jsonObject.put("bill_month", bill_month)
            jsonObject.put("total_size", total_size)
//            val items = netsData[d].value<JsonArray>(bill_month.toString())
            // 暂时先不加载上网流量数据
            val items = JsonArray()

            jsonObject.put("items", items)
            nets.add(jsonObject)
        }
        return nets
    }


    fun userInfoInto(userInfoList: List<JsonObject>): JsonObject {
        var userInfos = JsonObject()

        userInfoList.mapNotNull { json ->
            println("userInfoInto json-----:$json")
            val dataValue = json.value<JsonArray>("data")
            dataValue?.mapNotNull {
                it as JsonObject
                val jsonObject = JsonObject()

                jsonObject.put("last_modify_time", it.value<String>("last_modify_time")) // 转时间格式
                jsonObject.put("reliability", it.value<String>("reliability"))
                jsonObject.put("open_time", it.value<String>("open_time"))
                jsonObject.put("imsi", it.value<String>("imsi"))
                jsonObject.put("available_balance", it.getValue("available_balance")) //转int
                jsonObject.put("province", it.value<String>("province")) // 省份名称
                jsonObject.put("city", it.value<String>("city")) // 城市名称
                jsonObject.put("real_balance", it.getValue("real_balance")) //
                jsonObject.put("email", it.value<String>("email")) //
                jsonObject.put("address", it.value<String>("address")) //
                jsonObject.put("level", it.value<String>("level")) //
                jsonObject.put("mobile", it.value<String>("mobile")) //
                jsonObject.put("carrier", it.value<String>("carrier")) //
                jsonObject.put("idcard", it.value<String>("idcard")) //
                jsonObject.put("name", it.value<String>("name")) //
                jsonObject.put("package_name", it.value<String>("package_name")) //
                userInfos = jsonObject
            }

        }
        return userInfos
    }


    fun billsInto(billsList: List<JsonObject>): List<JsonObject> {
        val billsIntos = ArrayList<JsonObject>()

        billsList.mapNotNull { json ->
            println("billsInto json-----:$json")
            val dataValue = json.value<JsonArray>("data")
            println("dataValue：======= $dataValue")
            dataValue?.mapNotNull {
                it as JsonObject
                val jsonObject = JsonObject()

                jsonObject.put("bill_month", it.value<String>("bill_month"))
                jsonObject.put("bill_start_date", it.value<String>("bill_start_date")?.let { it->it.substring(0,4)+"-"+it.substring(4,6)+"-"+it.substring(6,8) }?:"") // 格式调整
                jsonObject.put("bill_end_date", it.value<String>("bill_end_date")?.let { it->it.substring(0,4)+"-"+it.substring(4,6)+"-"+it.substring(6,8) }?:"") // 格式调整
                jsonObject.put("bass_fee", it.getValue("bass_fee") ?: 0)
                jsonObject.put("extra_fee", it.getValue("extra_fee") ?: 0)
                jsonObject.put("voice_fee", it.getValue("voice_fee") ?: 0)
                jsonObject.put("sms_fee", it.getValue("sms_fee") ?: 0)
                jsonObject.put("total_fee", it.getValue("total_fee") ?: 0)
                jsonObject.put("discount", it.getValue("discount") ?: 0)
                jsonObject.put("extra_discount_fee", it.getValue("extra_discount_fee") ?: 0)
                jsonObject.put("actual_fee", it.getValue("actual_fee") ?: 0)
                jsonObject.put("paid_fee", it.getValue("paid_fee") ?: 0)
                jsonObject.put("unpaid_fee", it.getValue("unpaid_fee") ?: 0)
                jsonObject.put("point", it.getValue("point") ?: 0)
                jsonObject.put("last_point", it.getValue("last_point") ?: 0)
                jsonObject.put("related_mobiles", it.value<String>("related_mobiles") ?: "")
                jsonObject.put("notes", it.value<String>("notes") ?: "")
                billsIntos.add(jsonObject)
            }

        }
        return billsIntos
    }

    fun packagesInto(packagesData: List<JsonObject>, packagesCount: List<JsonObject>): ArrayList<JsonObject> {

        val nets = ArrayList<JsonObject>()

        (0..packagesData.size - 1).map { d ->
            val jsonObject = JsonObject()
            val data = packagesCount[d].value<JsonObject>("data")
            println("packagesInto ====：$data")

            val bill_start_date = data?.value<String>("bill_start_date") ?: ""
            val bill_end_date = data?.value<String>("bill_end_date") ?: ""
            jsonObject.put("bill_start_date", bill_start_date)
            jsonObject.put("bill_end_date", bill_end_date)
            val items = packagesData[d].value<JsonArray>("items")

            jsonObject.put("items", items)
            nets.add(jsonObject)
        }
        return nets
    }


// =====================================================================================================================

    /**
     *  查询短信记录原始数据 smses
     */
    fun findSmss(mobile: String, taskId: String): Observable<MutableList<List<JsonObject>>> {
        val queryCallLogRaw6Month = smsInfoDao.querySmsRaw6Month(mobile, taskId)
        val queryCallLogCountRaw6Month = smsInfoDao.querySmsCountRaw6Month(mobile, taskId)

        return Observable.concat(
            listOf(
                queryCallLogRaw6Month.toObservable(),
                queryCallLogCountRaw6Month.toObservable()
            )
        ).toList()

    }


    /**
     *  查询通话记录原始数据 calls
     */
    fun findCallLog(mobile: String, taskId: String): Observable<MutableList<List<JsonObject>>> {
        val queryCallLogRaw6Month = callLogDao.queryCallLogRaw6Month(mobile, taskId)
        val queryCallLogCountRaw6Month = callLogDao.queryCallLogCountRaw6Month(mobile, taskId)

        return Observable.concat(
            listOf(
                queryCallLogRaw6Month.toObservable(),
                queryCallLogCountRaw6Month.toObservable()
            )
        ).toList()

    }

    /**
     *  充值记录原始数据 recharges
     */
    fun findRecharges(mobile: String, taskId: String): Observable<MutableList<List<JsonObject>>> {
        val queryCallLogRaw6Month = paymentRecordDao.queryRechargesRaw6Month(mobile, taskId)

        return Observable.concat(listOf(queryCallLogRaw6Month.toObservable())).toList().toList()

    }

    /**
     *  查询上网流量记录原始数据 nets
     */
    fun findNets(mobile: String, taskId: String): Observable<MutableList<List<JsonObject>>> {
        val queryCallLogRaw6Month = internetInfoDao.queryNetsRaw6Month(mobile, taskId)
        val queryCallLogCountRaw6Month = internetInfoDao.queryNetsCountRaw6Month(mobile, taskId)

        return Observable.concat(
            listOf(
                queryCallLogRaw6Month.toObservable(),
                queryCallLogCountRaw6Month.toObservable()
            )
        ).toList()

    }

    /**
     *  用户信息记录 原始数据 carrier_user_info
     */
    fun findUserInfo(mobile: String, taskId: String): Observable<MutableList<List<JsonObject>>> {
        val queryUserInfoRaw6Month = userInfoDao.queryUserInfoRaw6Month(mobile, taskId)

        return Observable.concat(listOf(queryUserInfoRaw6Month.toObservable())).toList().toList()

    }

    /**
     *  月账单数据记录原始数据 recharges
     */
    fun findExpenseCalendar(mobile: String, taskId: String): Observable<MutableList<List<JsonObject>>> {
        val queryCallLogRaw6Month = expenseCalendarDao.queryBillsRaw6Month(mobile, taskId)

        return Observable.concat(listOf(queryCallLogRaw6Month.toObservable())).toList()

    }

    /**
     *  套餐数据记录原始数据 combo
     */
    fun findCombo(mobile: String, taskId: String): Observable<MutableList<List<JsonObject>>> {
        val queryComboRaw6Month = comboDao.queryComboRaw6Month(mobile, taskId)
        val queryComboCountRaw6Month = comboDao.queryComboCountRaw6Month(mobile, taskId)

        return Observable.concat(listOf(queryComboRaw6Month.toObservable(), queryComboCountRaw6Month.toObservable()))
            .toList()

    }

}
