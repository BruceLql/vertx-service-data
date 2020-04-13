package kavi.tech.service.service

import io.vertx.core.json.JsonObject
import kavi.tech.service.mysql.dao.CarrierResultDataDao
import kavi.tech.service.mysql.entity.CallLog
import kavi.tech.service.mysql.entity.CarrierResultData
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

/**
 * @packageName kavi.tech.service.service
 * @author litiezhu
 * @date 2020/4/13 18:21
 * @Description
 * <a href="goodmanalibaba@foxmail.com"></a>
 * @Versin 1.0
 */
@Service
class CallAnalysis {

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
                        var countLessOneMonth: Int = countLessOneMonth(mobile,task_id)//近1月主叫通话次数（近1月是指近30天，即0-30天）
                        var countLessThreeMonth: Int = countLessThreeMonth(mobile,task_id)//近3月主叫通话次数（近3月是指近三月的数据，即0-90天）
                        var countLessSixMonth: Int = countLessSixMonth(mobile,task_id)//近6月主叫通话次数（近6月是指近六月的数据，即0-180天）
                        var countLessThreeMonthAvg: Int = countLessThreeMonth(mobile,task_id)/3 // 近3月主叫月均通话次数
                        var countLessSixMonthAvg: Int = countLessSixMonth(mobile,task_id)/6 // 近6月主叫月均通话次数
                        var countLessOneMonthTime: Int = countLessOneMonthTime(mobile,task_id)// 近1月主叫通话时长
                        var countLessThreeMonthTime: Int = countLessThreeMonthTime(mobile,task_id)// 近3月主叫通话时长
                        var countLessSixMonthTime: Int = countLessSixMonthTime(mobile,task_id)// 近6月主叫通话时长

                        var countLessThreeMonthTimeAvg: Int = countLessThreeMonthTime(mobile,task_id)/3 // 近3月主叫月均通话时长（秒）
                        var countLessSixMonthTimeAvg: Int = countLessSixMonthTime(mobile,task_id)/6 // 近6月主叫月均通话时长（秒）（秒）

                        var becountLessOneMonth: Int = becountLessOneMonth(mobile,task_id) // 近1个月被叫通话次数
                        var becountLessThreeMonth: Int = becountLessThreeMonth(mobile,task_id) // 近3个月被叫通话次数
                        var becountLessSixMonth: Int = becountLessSixMonth(mobile,task_id) // 近6个月被叫通话次数
                        var becountLessThreeMonthAvg: Int = becountLessThreeMonth(mobile,task_id)/3 // 近3月被叫月均通话次数
                        var becountLessSixMonthAvg: Int = becountLessSixMonth(mobile,task_id)/6 // 近6个月被叫通话次数


                        var countLessOneMonthAll: Int = countLessOneMonthAll(mobile,task_id) // 近1月通话次数
                        var countLessThreeMonthAll: Int = countLessThreeMonthAll(mobile,task_id) // 近3月通话次数
                        var countLessSixMonthAll: Int = countLessSixMonthAll(mobile,task_id) // 近6月通话次数

                        var countLessThreeMonthAllAvg: Int = countLessThreeMonthAll /3 // 近3月平均通话次数
                        var countLessSixMonthAllAvg: Int = countLessSixMonthAll/6 // 近6月平均通话次数


                        var countLessOneMonthAllTime: Int = countLessOneMonthAllTime(mobile,task_id) // 近1月通话时长
                        var countLessThreeMonthAllTime: Int = countLessThreeMonthAllTime(mobile,task_id) // 近3月通话时长

                        var countLessSixMonthAllTime: Int = countLessSixMonthAllTime(mobile,task_id) // 近6月通话时长

                        var countLessThreeMonthAllTimeAvg: Int = countLessThreeMonthAllTime/3 // 近3月平均通话时长（秒）
                        var countLessSixMonthAllTimeAvg: Int = countLessSixMonthAllTime/6// 近6月平均通话时长（秒）








                    }
                }
        }
    }

    /***
     *
     * 近1月主叫通话次数
     *
     * 近1月通话总次数（近1月是指近30天，即0-30天）
     */
    fun countLessOneMonth(mobile: String,taskId: String ): Int {
        var sql: String =
                "SELECT\n" +
                "COUNT(id)\n" +
                "FROM\n" +
                "\tcarrier_voicecall\n" +
                "where \n" +
                " DATE(date_add(now(), interval -30 day))<\n" +
                "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                "and  mobile = \n" + mobile
                "and task_id = \"" + taskId
        var result: Int = 0
        carrierResultDataDao.customizeSQL(sql).subscribe { list ->
            var first: Int = list.size
            result = first
        }
        return result

    }

    /***
     *
     * 近1个月被叫通话次数
     *
     * 近1个月被叫通话次数（近1月是指近30天，即0-30天）
     */
    fun becountLessOneMonth(mobile: String,taskId: String ): Int {
        var sql: String =
            "SELECT\n" +
                    "COUNT(id)\n" +
                    "FROM\n" +
                    "\tcarrier_voicecall\n" +
                    "where \n" +
                    " DATE(date_add(now(), interval -30 day))<\n" +
                    "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                    "and  peer_number = \n" + mobile
        "and task_id = \"" + taskId
        var result: Int = 0
        carrierResultDataDao.customizeSQL(sql).subscribe { list ->
            var first: Int = list.size
            result = first
        }
        return result

    }

    /***
     * 近3月主叫通话次数
     *
     * 近3月通话总次数（近3月是指近三月的数据，即0-90天）
     */
    fun countLessThreeMonth(mobile: String,taskId: String ): Int {
        var sql: String =
            "SELECT\n" +
                    "COUNT(id)\n" +
                    "FROM\n" +
                    "\tcarrier_voicecall\n" +
                    "where \n" +
                    " DATE(date_add(now(), interval -90 day))<\n" +
                    "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                    "and  mobile = \n" + mobile
        "and task_id = \"" + taskId
        var result: Int = 0
        carrierResultDataDao.customizeSQL(sql).subscribe { list ->
            var first: Int = list.size
            result = first
        }
        return result

    }

    /***
     * 近3个月被叫通话次数
     *
     * 近3个月被叫通话次数（近3月是指近三月的数据，即0-90天）
     */
    fun becountLessThreeMonth(mobile: String,taskId: String ): Int {
        var sql: String =
            "SELECT\n" +
                    "COUNT(id)\n" +
                    "FROM\n" +
                    "\tcarrier_voicecall\n" +
                    "where \n" +
                    " DATE(date_add(now(), interval -90 day))<\n" +
                    "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                    "and  peer_number = \n" + mobile
        "and task_id = \"" + taskId
        var result: Int = 0
        carrierResultDataDao.customizeSQL(sql).subscribe { list ->
            var first: Int = list.size
            result = first
        }
        return result

    }

    /***
     * 近6月主叫月均通话次数（近6月是指近六月的数据，即0-180天）
     */
    fun becountLessSixMonth(mobile: String,taskId: String ): Int {
        var sql: String =
            "SELECT\n" +
                    "COUNT(id)\n" +
                    "FROM\n" +
                    "\tcarrier_voicecall\n" +
                    "where \n" +
                    " DATE(date_add(now(), interval -180 day))<\n" +
                    "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                    "and  peer_number = \n" + mobile
        "and task_id = \"" + taskId
        var result: Int = 0
        carrierResultDataDao.customizeSQL(sql).subscribe { list ->
            var first: Int = list.size
            result = first
        }
        return result
    }

    /***
     * 近6月主叫月均通话次数（近6月是指近六月的数据，即0-180天）
     */
    fun countLessSixMonth(mobile: String,taskId: String ): Int {
        var sql: String =
            "SELECT\n" +
                    "COUNT(id)\n" +
                    "FROM\n" +
                    "\tcarrier_voicecall\n" +
                    "where \n" +
                    " DATE(date_add(now(), interval -180 day))<\n" +
                    "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                    "and  mobile = \n" + mobile
        "and task_id = \"" + taskId
        var result: Int = 0
        carrierResultDataDao.customizeSQL(sql).subscribe { list ->
            var first: Int = list.size
            result = first
        }
        return result
    }


    /**
     *近1月主叫通话时长（秒）
     */
    fun countLessOneMonthTime(mobile: String,taskId: String ): Int {
        var sql: String =
                "SELECT\n" +
                "SUM(duration_in_second) as countTime\n" +
                "FROM\n" +
                "\tcarrier_voicecall\n" +
                "where \n" +
                " DATE(date_add(now(), interval -30 day))<\n" +
                "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                "and  mobile = " + mobile
                "and task_id = " + taskId
        var result: Int = 0
        carrierResultDataDao.customizeSQL(sql).subscribe { list ->
            var first: Int = list[0]?.getInteger("countTime")
            result = first
        }
        return result
    }

    /**
     *近3月主叫通话时长（秒）
     */
    fun countLessThreeMonthTime(mobile: String,taskId: String ): Int {
        var sql: String =
            "SELECT\n" +
                    "SUM(duration_in_second) as countTime\n" +
                    "FROM\n" +
                    "\tcarrier_voicecall\n" +
                    "where \n" +
                    " DATE(date_add(now(), interval -90 day))<\n" +
                    "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                    "and  mobile = " + mobile
        "and task_id = " + taskId
        var result: Int = 0
        carrierResultDataDao.customizeSQL(sql).subscribe { list ->
            var first: Int = list[0]?.getInteger("countTime")
            result = first
        }
        return result
    }

    /**
     *近6月主叫通话时长（秒）
     */
    fun countLessSixMonthTime(mobile: String,taskId: String ): Int {
        var sql: String =
            "SELECT\n" +
                    "SUM(duration_in_second) as countTime\n" +
                    "FROM\n" +
                    "\tcarrier_voicecall\n" +
                    "where \n" +
                    " DATE(date_add(now(), interval -180 day))<\n" +
                    "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                    "and  mobile = " + mobile
        "and task_id = " + taskId
        var result: Int = 0
        carrierResultDataDao.customizeSQL(sql).subscribe { list ->
            var first: Int = list[0]?.getInteger("countTime")
            result = first
        }
        return result
    }

    /**
     *近1月通话次数
     */
    fun countLessOneMonthAll(mobile: String,taskId: String ): Int {
        var sql: String =
                "SELECT\n" +
                "COUNT(*) as coutnTime \n" +
                "\n" +
                "FROM\n" +
                "\tcarrier_voicecall\n" +
                "where \n" +
                " (\n" +
                "\n" +
                " DATE(date_add(now(), interval -30 day))<\n" +
                "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                "and  \n" +
                "mobile = \n" + mobile
                "and task_id =  " + taskId
                "\n" +
                ") or (\n" +
                "\n" +
                " DATE(date_add(now(), interval -30 day))<\n" +
                "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                "and  \n" +
                "\n" +
                "peer_number = \n"  + mobile
                "and task_id =  " + taskId
                ")\n"

        var result: Int = 0
        carrierResultDataDao.customizeSQL(sql).subscribe { list ->
            var first: Int = list[0]?.getInteger("countTime")
            result = first
        }
        return result
    }

    /**
     *近3月通话次数
     */
    fun countLessThreeMonthAll(mobile: String,taskId: String ): Int {
        var sql: String =
            "SELECT\n" +
                    "COUNT(*) as coutnTime \n" +
                    "\n" +
                    "FROM\n" +
                    "\tcarrier_voicecall\n" +
                    "where \n" +
                    " (\n" +
                    "\n" +
                    " DATE(date_add(now(), interval -90 day))<\n" +
                    "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                    "and  \n" +
                    "mobile = \n" + mobile
        "and task_id =  " + taskId
        "\n" +
                ") or (\n" +
                "\n" +
                " DATE(date_add(now(), interval -90 day))<\n" +
                "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                "and  \n" +
                "\n" +
                "peer_number = \n"  + mobile
        "and task_id =  " + taskId
        ")\n"

        var result: Int = 0
        carrierResultDataDao.customizeSQL(sql).subscribe { list ->
            var first: Int = list[0]?.getInteger("countTime")
            result = first
        }
        return result
    }

    /**
     *近6月通话次数
     */
    fun countLessSixMonthAll(mobile: String,taskId: String ): Int {
        var sql: String =
            "SELECT\n" +
                    "COUNT(*) as coutnTime \n" +
                    "\n" +
                    "FROM\n" +
                    "\tcarrier_voicecall\n" +
                    "where \n" +
                    " (\n" +
                    "\n" +
                    " DATE(date_add(now(), interval -180 day))<\n" +
                    "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                    "and  \n" +
                    "mobile = \n" + mobile
        "and task_id =  " + taskId
        "\n" +
                ") or (\n" +
                "\n" +
                " DATE(date_add(now(), interval -180 day))<\n" +
                "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                "and  \n" +
                "\n" +
                "peer_number = \n"  + mobile
        "and task_id =  " + taskId
        ")\n"

        var result: Int = 0
        carrierResultDataDao.customizeSQL(sql).subscribe { list ->
            var first: Int = list[0]?.getInteger("countTime")
            result = first
        }
        return result
    }

    /**
     *近1月通话时长（秒）
     */
    fun countLessOneMonthAllTime(mobile: String,taskId: String ): Int {
        var sql: String = "\n" +
                "SELECT\n" +
                "SUM(duration_in_second) as countTime\n" +
                "\n" +
                "FROM\n" +
                "\tcarrier_voicecall\n" +
                "where \n" +
                " (\n" +
                "\n" +
                " DATE(date_add(now(), interval -30 day))<\n" +
                "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                "and  \n" +
                "mobile =  " + mobile
                "and task_id =  " + taskId
                "\n" +
                ") or (\n" +
                "\n" +
                " DATE(date_add(now(), interval -30 day))<\n" +
                "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                "and  \n" +
                "\n" +
                "peer_number =  " + mobile
                "and task_id =  " + taskId
                ")\n"

        var result: Int = 0
        carrierResultDataDao.customizeSQL(sql).subscribe { list ->
            var first: Int = list[0]?.getInteger("countTime")
            result = first
        }
        return result
    }
    /**
     *近3月通话时长（秒）（秒）
     */
    fun countLessThreeMonthAllTime(mobile: String,taskId: String ): Int {
        var sql: String = "\n" +
                "SELECT\n" +
                "SUM(duration_in_second) as countTime\n" +
                "\n" +
                "FROM\n" +
                "\tcarrier_voicecall\n" +
                "where \n" +
                " (\n" +
                "\n" +
                " DATE(date_add(now(), interval -90 day))<\n" +
                "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                "and  \n" +
                "mobile =  " + mobile
                "and task_id =  " + taskId
                "\n" +
                ") or (\n" +
                "\n" +
                " DATE(date_add(now(), interval -90 day))<\n" +
                "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                "and  \n" +
                "\n" +
                "peer_number =  " + mobile
                "and task_id =  " + taskId
                ")\n"

        var result: Int = 0
        carrierResultDataDao.customizeSQL(sql).subscribe { list ->
            var first: Int = list[0]?.getInteger("countTime")
            result = first
        }
        return result
    }

    /**
     *近6月通话时长（秒）（秒）（秒）
     */
    fun countLessSixMonthAllTime(mobile: String,taskId: String ): Int {
        var sql: String = "\n" +
                "SELECT\n" +
                "SUM(duration_in_second) as countTime\n" +
                "\n" +
                "FROM\n" +
                "\tcarrier_voicecall\n" +
                "where \n" +
                " (\n" +
                "\n" +
                " DATE(date_add(now(), interval -180 day))<\n" +
                "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                "and  \n" +
                "mobile =  " + mobile
        "and task_id =  " + taskId
        "\n" +
                ") or (\n" +
                "\n" +
                " DATE(date_add(now(), interval -180 day))<\n" +
                "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                "and  \n" +
                "\n" +
                "peer_number =  " + mobile
        "and task_id =  " + taskId
        ")\n"

        var result: Int = 0
        carrierResultDataDao.customizeSQL(sql).subscribe { list ->
            var first: Int = list[0]?.getInteger("countTime")
            result = first
        }
        return result
    }




}