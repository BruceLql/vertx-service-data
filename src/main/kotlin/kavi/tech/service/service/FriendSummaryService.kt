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
            carrierResultData.mobile = i.getString("mobile")
            carrierResultData.task_id = i.getString("task_id")

            carrierResultDataDao.selectBeforeInsert(carrierResultData)
                .subscribe { list ->
                    if (list.isNotEmpty()) {
                        val callLogList = list.map { it.mapTo(CallLog::class.java) }
                        var countLess3Months: Long = countLess3Months(callLogList)//统计 近90天月联系人数量（去重）(0-90天)
                        var countLess3MonthsGoupy:Int = countLess3MonthsGoupy(callLogList)//近90天联系人数量（联系10次以上，去重）（0-90天）
                        var countLess3Attribution:String = countLess3Attribution()//近90天联系次数最多的号码归属地（0-90天）
                        var attributionMobilePhoneNumber:Boolean = attributionMobilePhoneNumber()//近90天朋友圈中心城市是否与手机归属地一致（0-90天）

                        //TODO

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
    fun countLess3Attribution(): String {
        var sql: String = "SELECT\n" +
                "\tMAX(location)\n" +
                "FROM\n" +
                "\tcarrier_voicecall\n" +
                "where \n" +
                " DATE(date_add(now(), interval -90 day))<\n" +
                "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))"
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
    fun attributionMobilePhoneNumber(): Boolean {
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
                " DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time)))\n" +
                "as b\n" +
                " WHERE carrier_voicecall.mobile = b.mobile \n" +
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

}