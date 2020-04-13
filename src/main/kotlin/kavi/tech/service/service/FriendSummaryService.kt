package kavi.tech.service.service

import io.vertx.core.json.JsonObject
import kavi.tech.service.mysql.dao.CarrierResultDataDao
import kavi.tech.service.mysql.entity.CallLog
import kavi.tech.service.mysql.entity.CarrierResultData
import org.joda.time.DateTime
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import java.text.SimpleDateFormat
import java.util.*
import java.util.stream.Collectors

/**
 * @packageName kavi.tech.service.service
 * @author litiezhu
 * @date 2020/4/13 17:56
 * @Description
 * <a href="goodmanalibaba@foxmail.com"></a>
 * @Versin 1.0
 */
@Service
class FriendSummaryService {
    @Autowired
    private lateinit var carrierResultDataDao: CarrierResultDataDao


    fun toCleaningCircleFriendsData(data: List<JsonObject>) {
        if (data.isEmpty()) {
            throw IllegalAccessException("数据为空！")
        }
        data.forEach { i ->
            val carrierResultData = CarrierResultData()
            var mobile = i.getString("mobile")
            carrierResultData.mobile = mobile
            var task_id = i.getString("task_id")
            carrierResultData.task_id = task_id

            carrierResultDataDao.selectBeforeInsert(carrierResultData)
                .subscribe { list ->
                    if (list.isNotEmpty()) {
                        val callLogList = list.map { it.mapTo(CallLog::class.java) }
                        var countLess3Months: Long = countLess3Months(callLogList)//统计 近90天月联系人数量（去重）(0-90天)
                        var countLess3MonthsGoupy:Int = countLess3MonthsGoupy(callLogList)//近90天联系人数量（联系10次以上，去重）（0-90天）
                        var countLess3Attribution:String = countLess3Attribution(mobile,task_id)//近90天联系次数最多的号码归属地（0-90天）
                        var attributionMobilePhoneNumber:Boolean = attributionMobilePhoneNumber(mobile,task_id)//近90天朋友圈中心城市是否与手机归属地一致（0-90天）
                        var contactPerson:Int = contactPerson(mobile,task_id)//近90天互有主叫和被叫的联系人电话号码数目（去重）
                        var contactPersonTen:Int = contactPersonTen(mobile,task_id)//近180天的联系人数量（联系10次以上，去重）（0-180天）
                        var contactPersonTenHomeArea:String = contactPersonTenHomeArea(mobile,task_id)//近180天的联系次数最多的号码归属地（0-180天）
                        var attributionMobilePhoneNumberHun:Boolean = attributionMobilePhoneNumberHun(mobile,task_id)//近180天的朋友圈中心城市是否与手机归属地一致（0-180天）
                        var contactPersonHun:Int = contactPersonHun(mobile,task_id)//近180天的互有主叫和被叫的联系人电话号码数目（去重）（0-180天）
                    }
                }
        }
    }

    /**
     * 统计 近90天月联系人数量（去重）(0-90天)
     */
    fun countLess3Months(callLogList: List<CallLog>): Long {
        var plusMonths: Date = DateTime().plusMonths(3).toDate()
        return callLogList.parallelStream()
            .filter { it ->
                var parseDate: Date =
                    SimpleDateFormat("yyyyMMdd HH:mm:ss").parse(
                        it.bill_month?.substring(
                            0,
                            3
                        ) + "-" + it.time
                    )

                null != it?.peer_number && parseDate > plusMonths
            }
            .distinct()
            .count()
    }

    /**
     * 近90天联系人数量（联系10次以上，去重）（0-90天）
     */
    fun countLess3MonthsGoupy(callLogList: List<CallLog>): Int {

        var plusMonths: Date = DateTime().plusMonths(3).toDate()
        callLogList.run {
            /** 筛选出过去九十天*/
            var collectMiddleList = parallelStream()
                .filter { it ->
                    var parseDate: Date =
                        SimpleDateFormat("yyyyMM HH:mm:ss").parse(
                            it.bill_month?.substring(
                                0,
                                3
                            ) + "-" + it.time
                        )

                    null != it?.peer_number && parseDate > plusMonths
                }
                .distinct()
                .collect(Collectors.toList())

            var collectToFinalResult = collectMiddleList.parallelStream()
                .collect(Collectors.groupingBy(CallLog::mobile, Collectors.counting()))
            /** 筛选出 大于10次的*/
            var iterator = collectToFinalResult.iterator()
            while (iterator.hasNext()) {
                var next = iterator.next()
                if (next.value < 10) {
                    iterator.remove()
                }
            }
            var conformRulesKMobileList: List<String?> = collectToFinalResult.keys.toList()

            collectMiddleList = collectMiddleList.stream().filter {
                conformRulesKMobileList.contains(it.mobile)
            }.collect(Collectors.toList())
            return collectMiddleList.size
        }

    }

    /***
     * 近90天联系次数最多的号码归属地（0-90天）
     */
    fun countLess3Attribution(mobile: String,taskId: String ): String {
        var sql: String = "SELECT\n" +
                "\tMAX(location)\n" +
                "FROM\n" +
                "\tcarrier_voicecall\n" +
                "where \n" +
                " DATE(date_add(now(), interval -90 day))<\n" +
                "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))" +
                "AND mobile = " + mobile
                "AND task_id = " + taskId
        var result: JsonObject = JsonObject()
        carrierResultDataDao.customizeSQL(sql).subscribe { list ->
            var first: JsonObject = list.first()
            result = first
        }
        return result.toString()

    }

    /***
     * 近90天朋友圈中心城市是否与手机归属地一致（0-90天）
     */
    fun attributionMobilePhoneNumber(mobile: String,taskId: String ): Boolean {
        var sql: String = "SELECT \n" +
                "(\n" +
                "CASE\n" +
                "WHEN carrier_baseinfo.city is null THEN 0\n" +
                "WHEN c.homearea is null THEN 0\n" +
                "WHEN carrier_baseinfo.city  <=> c.homearea  THEN true\n" +
                "ELSE FALSE\n" +
                "END\n" +
                ")\n" +
                " as result" +
                "from \n" +
                "(\n" +
                "SELECT carrier_voicecall.homearea, b.mobile from  carrier_voicecall ,\n" +
                "(\n" +
                "SELECT\n" +
                "\tMAX(mobile) as mobile\n" +
                "FROM\n" +
                "\tcarrier_voicecall\n" +
                "where \n" +
                " DATE(date_add(now(), interval -90 day))<\n" +
                " DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))" +
                " and carrier_voicecall.mobile = \n" + mobile
                " and carrier_voicecall.task_id = " +  taskId
                ")\n" +
                "as b\n" +
                " WHERE carrier_voicecall.mobile = b.mobile \n" +
                        " and carrier_voicecall.mobile = \n" + mobile
                 " and carrier_voicecall.task_id = " +  taskId
                ") as c \n" +
                "LEFT JOIN carrier_baseinfo on c.mobile = carrier_baseinfo.mobile\n" +
                "LIMIT 1 "
        var result: JsonObject = JsonObject()
        carrierResultDataDao.customizeSQL(sql).subscribe { list ->
            var first: JsonObject = list.first()
            result = first
        }
        return result.getBoolean("result")
    }

    /***
     *  近90天互有主叫和被叫的联系人电话号码数目（去重）（0-90天）
     */
    fun contactPerson(mobile: String,taskId: String): Int {
        var sql: String = "SELECT DISTINCT carrier_voicecall.mobile from carrier_voicecall ,\n" +
                "(\n" +
                "SELECT DISTINCT peer_number,mobile from carrier_voicecall\n" +
                "  where DATE(date_add(now(), interval -90 day))<\n" +
                " DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))" +

                ") as b where carrier_voicecall.mobile = b.peer_number \n" +
                "and  carrier_voicecall.peer_number = b.mobile " +
                "and b.mobile = " + mobile
                "and b.task_id =" + taskId +
                        " and  DATE(date_add(now(), interval -90 day))<\n" +
                        " DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))"
        var result: Int = 0
        carrierResultDataDao.customizeSQL(sql).subscribe { list ->
            var first: Int = list.size
            result = first
        }
        return result
    }

    /***
     *  近180天的联系人数量（联系10次以上，去重）（0-180天）
     */
    fun contactPersonTen(mobile: String,taskId: String): Int {
        var sql: String = "SELECT\n" +
                "mobile, peer_number\n" +
                "FROM\n" +
                "\tcarrier_voicecall\n" +
                "where \n" +
                " DATE(date_add(now(), interval -180 day))<\n" +
                "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                "and  mobile = \n" + mobile
                "and task_id = \n" + taskId
                "GROUP BY mobile HAVING(COUNT(DISTINCT(peer_number)) > 10)"
        var result: Int = 0
        carrierResultDataDao.customizeSQL(sql).subscribe { list ->
            var first: Int = list.size
            result = first
        }
        return result
    }
  /***
     * 近180天的联系次数最多的号码归属地（0-180天）
     */
    fun contactPersonTenHomeArea(mobile: String,taskId: String): String {
        var sql: String =  "\n" +
                "SELECT\n" +
                "homearea\n" +
                "FROM\n" +
                "\tcarrier_voicecall\n" +
                "where \n" +
                " DATE(date_add(now(), interval -180 day))<\n" +
                "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                "and  mobile = \n" + mobile
                "and task_id = \n" + taskId
                "GROUP BY mobile HAVING(MAX(DISTINCT(peer_number)))"
        var result: String = ""
        carrierResultDataDao.customizeSQL(sql).subscribe { list ->
            var first: String = list[0]?.toString()
            result = first
        }
        return result
    }

    /***
     * 近180天的朋友圈中心城市是否与手机归属地一致（0-180天）
     */
    fun attributionMobilePhoneNumberHun(mobile: String,taskId: String ): Boolean {
        var sql: String = "SELECT \n" +
                "(\n" +
                "CASE\n" +
                "WHEN carrier_baseinfo.city is null THEN 0\n" +
                "WHEN c.homearea is null THEN 0\n" +
                "WHEN carrier_baseinfo.city  <=> c.homearea  THEN true\n" +
                "ELSE FALSE\n" +
                "END\n" +
                ")\n" +
                " as result" +
                "from \n" +
                "(\n" +
                "SELECT carrier_voicecall.homearea, b.mobile from  carrier_voicecall ,\n" +
                "(\n" +
                "SELECT\n" +
                "\tMAX(mobile) as mobile\n" +
                "FROM\n" +
                "\tcarrier_voicecall\n" +
                "where \n" +
                " DATE(date_add(now(), interval -180 day))<\n" +
                " DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))" +
                " and carrier_voicecall.mobile = \n" + mobile
        " and carrier_voicecall.task_id = " +  taskId
        ")\n" +
                "as b\n" +
                " WHERE carrier_voicecall.mobile = b.mobile \n" +
                " and carrier_voicecall.mobile = \n" + mobile
        " and carrier_voicecall.task_id = " +  taskId
        ") as c \n" +
                "LEFT JOIN carrier_baseinfo on c.mobile = carrier_baseinfo.mobile\n" +
                "LIMIT 1 "
        var result: JsonObject = JsonObject()
        carrierResultDataDao.customizeSQL(sql).subscribe { list ->
            var first: JsonObject = list.first()
            result = first
        }
        return result.getBoolean("result")
    }

    /***
     *  近180天的互有主叫和被叫的联系人电话号码数目（去重）（0-180天）
     */
    fun contactPersonHun(mobile: String,taskId: String): Int {
        var sql: String = "SELECT DISTINCT carrier_voicecall.mobile from carrier_voicecall ,\n" +
                "(\n" +
                "SELECT DISTINCT peer_number,mobile from carrier_voicecall\n" +
                "  where DATE(date_add(now(), interval -180 day))<\n" +
                " DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))" +

                ") as b where carrier_voicecall.mobile = b.peer_number \n" +
                "and  carrier_voicecall.peer_number = b.mobile " +
                "and b.mobile = " + mobile
        "and b.task_id =" + taskId +
                " and  DATE(date_add(now(), interval -90 day))<\n" +
                " DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))"
        var result: Int = 0
        carrierResultDataDao.customizeSQL(sql).subscribe { list ->
            var first: Int = list.size
            result = first
        }
        return result
    }



}