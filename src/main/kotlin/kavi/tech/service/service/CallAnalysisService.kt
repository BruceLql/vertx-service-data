package kavi.tech.service.service

import io.vertx.codegen.CodeGenProcessor.log
import io.vertx.core.json.JsonObject
import io.vertx.ext.sql.ResultSet
import io.vertx.rxjava.ext.asyncsql.AsyncSQLClient
import io.vertx.rxjava.ext.sql.SQLConnection
import kavi.tech.service.mysql.dao.CarrierResultDataDao
import kavi.tech.service.mysql.entity.CarrierResultData
import org.apache.commons.lang3.StringUtils
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import rx.Observable
import rx.Single

/**
 * @packageName kavi.tech.service.service
 * @author litiezhu
 * @date 2020/4/13 18:21
 * @Description
 * <a href="goodmanalibaba@foxmail.com"></a>
 * @Versin 1.0
 */
@Service
class CallAnalysisService {

    @Autowired
    private lateinit var carrierResultDataDao: CarrierResultDataDao

    @Autowired
    private lateinit var client: AsyncSQLClient

    fun toCleaningCircleFriendsData(mobile: String, task_id: String): Single<List<ResultSet>> {
        if (StringUtils.isBlank(mobile) || StringUtils.isBlank(task_id)) {
            throw IllegalAccessException("数据为空！")
        }
//        val carrierResultDataList = ArrayList<JsonObject>()

//        val carrierResultData = CarrierResultData()
//        val carrierResultDataToInsertlist = ArrayList<CarrierResultData>()
//        carrierResultData.mobile = mobile
//        carrierResultData.task_id = task_id

//        val list = listOf(
//            countLessThreeMonthAllTime(mobile, task_id)
//                .map {
//                    JsonObject().put("call_time_3m", it)//近3月通话时长（秒）
//                }.toObservable(),
//            countLessSixMonthAllTime(mobile, task_id).map {
//                JsonObject().put("call_time_6m", it) // 近6月通话时长（秒）
//            }.toObservable(),
//            countLessThreeMonth(mobile, task_id).map {
//                JsonObject().put("call_dial_cnt_3m", it) //近3月主叫通话次数
//            }.toObservable(),
//            countLessSixMonth(mobile, task_id).map {
//                JsonObject().put("call_dial_cnt_6m", it)//近6月主叫通话次数
//            }.toObservable(),
//            countLessThreeMonthTime(mobile, task_id).map {
//                JsonObject().put("call_dial_time_3m", it) // 近3月主叫通话时长（近3月是指近三月的数据，即0-90天）
//            }.toObservable(),
//            countLessSixMonthTime(mobile, task_id).map {
//                JsonObject().put("call_dial_time_6m", it) //近6月主叫通话时长
//            }.toObservable(),
//            becountLessThreeMonth(mobile, task_id).map {
//                JsonObject().put("call_dialed_cnt_3m", it) // 近3个月被叫通话次数
//            }.toObservable(),
//            becountLessSixMonth(mobile, task_id).map {
//                JsonObject().put("call_dialed_cnt_6m", it) //近6月主叫月均通话次数  近6个月被叫通话次数
//            }.toObservable(),
//            countLessThreeMonthAll(mobile, task_id).map {
//                JsonObject().put("call_cnt_3m", it) //近3月通话次数
//            }.toObservable(),
//            countLessSixMonthAll(mobile, task_id).map {
//                JsonObject().put("call_cnt_6m", it)// 近6月通话次数
//            }.toObservable(),
//            countLessThreeMonthTime(mobile, task_id).map {
//                JsonObject().put("call_dial_time_3m", it)// 近3月主叫通话时长
//            }.toObservable(),
//            countLessSixMonthTime(mobile, task_id).map {
//                JsonObject().put("call_dial_time_6m", it)  // 近6月主叫通话时长
//            }.toObservable()
//        )
 /*       Observable.concat(list).toList()
            .subscribe(
                { item ->
                    item.map { json ->
                        var jsonObject: JsonObject = JsonObject()
                        val countLessThreeMonthAllTime = json.getInteger("call_time_3m") //近3月通话时长（秒）
                        val countLessSixMonthAllTime = json.getInteger("call_time_6m") // 近6月通话时长（秒）
                        val countLessThreeMonthAll = json.getInteger("call_cnt_3m") //近3月通话次数
                        val countLessSixMonthAll = json.getInteger("call_cnt_6m") // 近6月通话次数
                        val becountLessThreeMonth = json.getInteger("call_dialed_cnt_3m") // 近3个月被叫通话次数
                        val countLessSixMonth = json.getInteger("call_dial_cnt_6m") //近6月主叫通话次数
                        val becountLessSixMonth = json.getInteger("call_dialed_cnt_6m") //近6个月被叫通话次数
                        val countLessThreeMonth = json.getInteger("call_dial_cnt_3m")  //近3月主叫通话次数
                        val countLessThreeMonthTime = json.getInteger("call_dial_time_3m")  //近3月主叫通话时长
                        val countLessSixMonthTime = json.getInteger("call_dial_time_6m")  //近6月主叫通话时长

                        var countLessThreeMonthTimeAvg: Int = countLessThreeMonthTime?:0 / 3 // 近3月主叫月均通话时长（秒） --
                        var countLessSixMonthTimeAvg: Int = countLessSixMonthTime?:0 / 6 // 近6月主叫月均通话时长（秒）（秒） --

                        var countLessThreeMonthAllTimeAvg: Int = countLessThreeMonthAllTime.toInt()?:0 / 3 // 近3月平均通话时长（秒）
                        var countLessSixMonthAllTimeAvg: Int = countLessSixMonthAllTime?:0 / 6 // 近6月平均通话时长（秒）

                        var countLessThreeMonthAllAvg: Int = countLessThreeMonthAll?:0 / 3 // 近3月平均通话次数
                        var countLessSixMonthAllAvg: Int = countLessSixMonthAll?:0 / 6 // 近6月平均通话次数

                        var becountLessThreeMonthAvg: Int = becountLessThreeMonth?:0 / 3 // 近3月被叫月均通话次数 --
                        var becountLessSixMonthAvg: Int = becountLessSixMonth?:0 / 6 // 近6月被叫月均通话次数--
                        var countLessThreeMonthAvg: Int = countLessThreeMonth?:0 / 3 // 近3月主叫月均通话次数  --
                        var countLessSixMonthAvg: Int = countLessSixMonth?:0 / 6  // 近6月主叫月均通话次数 --


                            countLessOneMonth(mobile, task_id)//近1月主叫通话次数（近1月是指近30天，即0-30天）


                        countLessOneMonthTime(mobile, task_id)// 近1月主叫通话时长


                         becountLessOneMonth(mobile, task_id) // 近1个月被叫通话次数

                        countLessOneMonthAll(mobile, task_id) // 近1月通话次数
//                        var countLessThreeMonthAll: Single<Int> = countLessThreeMonthAll(mobile, task_id) // 近3月通话次数

                        countLessOneMonthAllTime(mobile, task_id) // 近1月通话时长


//                        jsonObject.put(" call_cnt_1m", countLessOneMonthAll.map { it.toInt() }) // 近1月通话次数
                        jsonObject.put("call_cnt_3m", countLessThreeMonthAll)  // 近3月通话次数
                        jsonObject.put("call_cnt_6m", countLessSixMonthAll) //近6月通话次数

                        jsonObject.put("avg_call_cnt_3m", countLessThreeMonthAllAvg) // 近3月平均通话次数

                        jsonObject.put("avg_call_cnt_6m", countLessSixMonthAllAvg) // 近6月平均通话次数

//                        jsonObject.put("call_time_1m", countLessOneMonthAllTime.map { it.toInt() }) // 近1月通话时长（秒）

                        jsonObject.put("call_time_3m ", countLessThreeMonthAllTime) // 近3月通话时长（秒）

                        jsonObject.put("call_time_6m", countLessSixMonthAllTime) // 近6月通话时长

                        jsonObject.put("avg_call_time_3m", countLessThreeMonthAllTimeAvg) // 近3月平均通话时长

                        jsonObject.put("avg_call_time_6m ", countLessSixMonthAllTimeAvg) //近6月平均通话时长

//                        jsonObject.put("call_dial_cnt_1m", countLessOneMonth.map { it.toInt() }) // 近1月主叫通话次数

                        jsonObject.put("call_dial_cnt_3m", countLessThreeMonth)//近3月主叫通话次数

                        jsonObject.put("call_dial_cnt_6m", countLessSixMonth)//近6月主叫通话次数

                        jsonObject.put("avg_call_dial_cnt_3m", countLessThreeMonthAvg)//近3月主叫月均通话次数

                        jsonObject.put("avg_call_dial_cnt_6m", countLessSixMonthAvg) //近6月主叫月均通话次数

//                        jsonObject.put("call_dial_time_1m", countLessOneMonthTime.map { it.toInt() })//近1月主叫通话时长

                        jsonObject.put("call_dial_time_3m", countLessThreeMonthTime)//近3月主叫通话时长

                        jsonObject.put("call_dial_time_6m", countLessSixMonthTime)//近6月主叫通话时长

                        jsonObject.put("avg_call_dial_time_3m", countLessThreeMonthTimeAvg)//近3月主叫月均通话时长

                        jsonObject.put("avg_call_dial_time_6m", countLessSixMonthTimeAvg)//近6月主叫月均通话时长

//                        jsonObject.put("call_dialed_cnt_1m", becountLessOneMonth.map { it.toInt() })//近1个月被叫通话次数

                        jsonObject.put("call_dialed_cnt_3m", becountLessThreeMonth)//近3个月被叫通话次数

                        jsonObject.put("call_dialed_cnt_6m", becountLessSixMonth)//近6个月被叫通话次数

                        jsonObject.put("avg_call_dialed_cnt_3m", becountLessThreeMonthAvg)//近3月被叫月均通话次数

                        jsonObject.put("avg_call_dialed_cnt_6m", becountLessSixMonthAvg)//近6月被叫月均通话次数
                        //TODO 少 小昌那边的 五个数据

                        carrierResultDataList.add(jsonObject)
                    }
                    carrierResultData.task_id = task_id
                    carrierResultData.mobile = mobile
                    carrierResultData.item = "call_risk_analysis"
                    carrierResultData.result = carrierResultDataList.toString()
                    carrierResultDataToInsertlist.add(carrierResultData)
                    //插入数据
                    carrierResultDataDao.insertBybatch(carrierResultData, carrierResultDataToInsertlist)
                }, {
                    it.printStackTrace()
                }
            )*/

        return this.client.rxGetConnection().flatMap { conn->
            Observable.concat(
                listOf(
                    countLessOneMonthAll(conn,mobile,task_id).toObservable(),
                    countLessThreeMonthAll(conn,mobile,task_id).toObservable(),
                    countLessSixMonthAll(conn,mobile,task_id).toObservable(),
                    countLessThreeMonthAllAvg(conn,mobile,task_id).toObservable(),
                    countLessSixMonthAllAvg(conn,mobile,task_id).toObservable(),
                    countLessOneMonthAllTime(conn,mobile,task_id).toObservable(),
                    countLessThreeMonthAllTime(conn,mobile,task_id).toObservable(),
                    countLessSixMonthAllTime(conn,mobile,task_id).toObservable(),
                    countLessThreeMonthAllTimeAvg(conn,mobile,task_id).toObservable(),
                    countLessSixMonthAllTimeAvg(conn,mobile,task_id).toObservable(),
                    countLessOneMonth(conn,mobile,task_id).toObservable(),
                    countLessThreeMonth(conn,mobile,task_id).toObservable(),
                    countLessSixMonth(conn,mobile,task_id).toObservable(),
                    countLessThreeMonthAvg(conn,mobile,task_id).toObservable(),
                    countLessSixMonthAvg(conn,mobile,task_id).toObservable(),
                    countLessOneMonthTime(conn,mobile,task_id).toObservable(),
                    countLessThreeMonthTime(conn,mobile,task_id).toObservable(),
                    countLessSixMonthTime(conn,mobile,task_id).toObservable(),
                    countLessThreeMonthTimeAvg(conn,mobile,task_id).toObservable(),

                    countLessSixMonthTimeAvg(conn,mobile,task_id).toObservable(),

                    becountLessOneMonth(conn,mobile,task_id).toObservable(),
                    becountLessThreeMonth(conn,mobile,task_id).toObservable(),
                    becountLessSixMonth(conn,mobile,task_id).toObservable(),
                    becountLessThreeMonthAvg(conn,mobile,task_id).toObservable(),
                    becountLessSixMonthAvg(conn,mobile,task_id).toObservable()

//                    contactPersonHun(conn,mobile,task_id).toObservable(),
//                    contactPersonHun(conn,mobile,task_id).toObservable(),
//                    contactPersonHun(conn,mobile,task_id).toObservable(),
//                    contactPersonHun(conn,mobile,task_id).toObservable(),
//                    contactPersonHun(conn,mobile,task_id).toObservable(),

                )
            ).toList().toSingle()
        }

    }



    /***
     *
     * 近1月主叫通话次数
     *
     * 近1月通话总次数（近1月是指近30天，即0-30天）
     */
    fun countLessOneMonth(conn: SQLConnection, mobile: String, taskId: String): Single<ResultSet> {
        log.info("近1月主叫通话次数")
        var sql: String =
            "SELECT\n" +
                    "COUNT(id) as call_dial_cnt_1m \n" +
                    "FROM\n" +
                    "\tcarrier_voicecall\n" +
                    "where \n" +
                    " DATE(date_add(now(), interval -30 day))<\n" +
                    "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                    "and  mobile = '$mobile' \n" +
        "and task_id = '$taskId' "
        return conn.rxQuery(sql).doAfterTerminate(conn::close)
            .doOnError {
                log.info("近1月主叫通话次数")
                print(it.printStackTrace())
            }
    }

    /***
     *
     * 近1个月被叫通话次数
     *
     * 近1个月被叫通话次数（近1月是指近30天，即0-30天）
     */
    fun becountLessOneMonth(conn:SQLConnection, mobile: String, taskId: String): Single<ResultSet> {
        log.info("近1个月被叫通话次数")
        var sql: String =
            "SELECT\n" +
                    "COUNT(id) as call_dialed_cnt_1m \n" +
                    "FROM\n" +
                    "\tcarrier_voicecall\n" +
                    "where \n" +
                    " DATE(date_add(now(), interval -30 day))<\n" +
                    "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                    "and  peer_number = '$mobile'  \n" +
        "and task_id = '$taskId'  "
        return conn.rxQuery(sql).doAfterTerminate(conn::close)
            .doOnError {
                log.info("近1个月被叫通话次数")
                print(it.printStackTrace())
            }
    }

    /***
     * 近3月主叫月均通话次数
     *
     */
    fun countLessThreeMonthAvg(conn:SQLConnection, mobile: String, taskId: String): Single<ResultSet> {
        log.info("近3月主叫月均通话次数")
        var sql: String =
            "SELECT\n" +
                    "COUNT(id) as avg_call_dial_cnt_3m\n" +
                    "FROM\n" +
                    "\tcarrier_voicecall\n" +
                    "where \n" +
                    " DATE(date_add(now(), interval -90 day))<\n" +
                    "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                    "and  mobile = '$mobile'  \n" +
        "and task_id = '$taskId' "

        return conn.rxQuery(sql).doAfterTerminate(conn::close)
            .doOnError {
                log.info("近3月主叫月均通话次数")
                print(it.printStackTrace())
            }
    }

    /***
     * 近3月主叫通话次数
     *
     */
    fun countLessThreeMonth(conn:SQLConnection, mobile: String, taskId: String): Single<ResultSet> {
        log.info("近3月主叫通话次数")
        var sql: String =
            "SELECT\n" +
                    "COUNT(id) as call_dial_cnt_3m\n" +
                    "FROM\n" +
                    "\tcarrier_voicecall\n" +
                    "where \n" +
                    " DATE(date_add(now(), interval -90 day))<\n" +
                    "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                    "and  mobile = '$mobile'  \n" +
                    "and task_id = '$taskId' "

        return conn.rxQuery(sql).doAfterTerminate(conn::close)
            .doOnError {
                log.info("近3月主叫通话次数")
                print(it.printStackTrace())
            }
    }

    /***
     * 近3个月被叫通话次数
     *
     * 近3个月被叫通话次数（近3月是指近三月的数据，即0-90天）
     */
    fun becountLessThreeMonth(conn:SQLConnection, mobile: String, taskId: String): Single<ResultSet> {
        log.info("近3个月被叫通话次数")
        var sql: String =
            "SELECT\n" +
                    "COUNT(id) as call_dialed_cnt_3m\n" +
                    "FROM\n" +
                    "\tcarrier_voicecall\n" +
                    "where \n" +
                    " DATE(date_add(now(), interval -90 day))<\n" +
                    "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                    "and  peer_number = '$mobile'  \n" +
        "and task_id = '$taskId' "
        return conn.rxQuery(sql).doAfterTerminate(conn::close)
            .doOnError {
                log.info("近3个月被叫通话次数")
                print(it.printStackTrace())
            }
    }

    /***
     * 近3月被叫月均通话次数
     *
     */
    fun becountLessThreeMonthAvg(conn:SQLConnection, mobile: String, taskId: String): Single<ResultSet> {
        log.info("近3月被叫月均通话次数")
        var sql: String =
            "SELECT\n" +
                    "COUNT(id) as avg_call_dialed_cnt_3m\n" +
                    "FROM\n" +
                    "\tcarrier_voicecall\n" +
                    "where \n" +
                    " DATE(date_add(now(), interval -90 day))<\n" +
                    "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                    "and  peer_number = '$mobile'  " +
                    "and task_id = '$taskId' "
        return conn.rxQuery(sql).doAfterTerminate(conn::close)
            .doOnError {
                log.info("近3月被叫月均通话次数")
                print(it.printStackTrace())
            }
    }

    /***
     * 近6个月被叫通话次数（近6月是指近六月的数据，即0-180天）
     */
    fun becountLessSixMonth(conn:SQLConnection, mobile: String, taskId: String): Single<ResultSet> {
        log.info("近6个月被叫通话次数（近6月是指近六月的数据，即0-180天）")
        var sql: String =
            "SELECT\n" +
                    "COUNT(id) AS call_dialed_cnt_6m\n" +
                    "FROM\n" +
                    "\tcarrier_voicecall\n" +
                    "where \n" +
                    " DATE(date_add(now(), interval -180 day))<\n" +
                    "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                    "and  peer_number = '$mobile'  \n" +
        "and task_id = '$taskId'  "
        return conn.rxQuery(sql).doAfterTerminate(conn::close)
            .doOnError {
                log.info("近6个月被叫通话次数")
                print(it.printStackTrace())
            }
    }

    /***
     * 近6月被叫月均通话次数
     */
    fun becountLessSixMonthAvg(conn:SQLConnection, mobile: String, taskId: String): Single<ResultSet> {
        log.info("近6个月被叫通话次数（近6月是指近六月的数据，即0-180天）")
        var sql: String =
            "SELECT\n" +
                    "COUNT(id) AS avg_call_dialed_cnt_6m\n" +
                    "FROM\n" +
                    "\tcarrier_voicecall\n" +
                    "where \n" +
                    " DATE(date_add(now(), interval -180 day))<\n" +
                    "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                    "and  peer_number = $mobile \n" +
                    "and task_id = '$taskId'  "
        return conn.rxQuery(sql).doAfterTerminate(conn::close)
            .doOnError {
                log.info("近6月被叫月均通话次数")
                print(it.printStackTrace())
            }
    }

    /***
     * 近6月主叫通话次数（近6月是指近六月的数据，即0-180天）
     */
    fun countLessSixMonth(conn:SQLConnection, mobile: String, taskId: String): Single<ResultSet> {
        log.info("近6月主叫通话次数（近6月是指近六月的数据，即0-180天）")
        var sql: String =
            "SELECT\n" +
                    "COUNT(id) as call_dial_cnt_6m\n" +
                    "FROM\n" +
                    "\tcarrier_voicecall\n" +
                    "where \n" +
                    " DATE(date_add(now(), interval -180 day))<\n" +
                    "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                    "and  mobile = '$mobile' \n" +
        "and task_id = '$taskId' "
        return conn.rxQuery(sql).doAfterTerminate(conn::close)
            .doOnError {
                log.info("近6月主叫通话次数")
                print(it.printStackTrace())
            }
    }

    /***
     * 近6月主叫通话次数（近6月是指近六月的数据，即0-180天）
     */
    fun countLessSixMonthAvg(conn:SQLConnection, mobile: String, taskId: String): Single<ResultSet> {
        log.info("近6月主叫通话次数（近6月是指近六月的数据，即0-180天）")
        var sql: String =
            "SELECT\n" +
                    "COUNT(id) as avg_call_dial_cnt_6m\n" +
                    "FROM\n" +
                    "\tcarrier_voicecall\n" +
                    "where \n" +
                    " DATE(date_add(now(), interval -180 day))<\n" +
                    "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                    "and  mobile = '$mobile' \n" +
                    "and task_id = '$taskId' "
        return conn.rxQuery(sql).doAfterTerminate(conn::close)
            .doOnError {
                log.info("近6月主叫通话次数")
                print(it.printStackTrace())
            }
    }


    /**
     *近1月主叫通话时长（秒）
     */
    fun countLessOneMonthTime(conn:SQLConnection, mobile: String, taskId: String): Single<ResultSet> {
        log.info("近1月主叫通话时长（秒）")
        var sql: String =
            "SELECT\n" +
                    "SUM(duration_in_second) as call_dial_time_1m\n" +
                    "FROM\n" +
                    "\tcarrier_voicecall\n" +
                    "where \n" +
                    " DATE(date_add(now(), interval -30 day))<\n" +
                    "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                    "and  mobile = '$mobile'  " +
        "and task_id = '$taskId'  "
        return conn.rxQuery(sql).doAfterTerminate(conn::close)
            .doOnError {
                log.info("近1月主叫通话时长")
                print(it.printStackTrace())
            }
    }

    /**
     *近3月主叫通话时长（秒）
     */
    fun countLessThreeMonthTime(conn:SQLConnection, mobile: String, taskId: String): Single<ResultSet> {
        log.info("近3月主叫通话时长（秒）")
        var sql: String =
            "SELECT\n" +
                    "SUM(duration_in_second) as call_dial_time_1m\n" +
                    "FROM\n" +
                    "\tcarrier_voicecall\n" +
                    "where \n" +
                    " DATE(date_add(now(), interval -90 day))<\n" +
                    "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                    "and  mobile = '$mobile'  " +
        "and task_id = '$taskId'  "
        return conn.rxQuery(sql).doAfterTerminate(conn::close)
            .doOnError {
                log.info("近3月主叫通话时长")
                print(it.printStackTrace())
            }
    }

    /**
     *近3月主叫月均通话时长
     */
    fun countLessThreeMonthTimeAvg(conn:SQLConnection, mobile: String, taskId: String): Single<ResultSet> {
        log.info("近3月主叫月均通话时长")
        var sql: String =
            "SELECT\n" +
                    "SUM(duration_in_second)/3 as avg_call_dial_time_3m\n" +
                    "FROM\n" +
                    "\tcarrier_voicecall\n" +
                    "where \n" +
                    " DATE(date_add(now(), interval -90 day))<\n" +
                    "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                    "and  mobile = '$mobile'  " +
                    "and task_id = '$taskId'  "
        return conn.rxQuery(sql).doAfterTerminate(conn::close)
            .doOnError {
                log.info("近3月主叫月均通话时长")
                print(it.printStackTrace())
            }
    }

    /**
     *近6月主叫通话时长（秒）
     */
    fun countLessSixMonthTime(conn:SQLConnection, mobile: String, taskId: String): Single<ResultSet> {
        log.info("近6月主叫通话时长（秒）")
        var sql: String =
            "SELECT\n" +
                    "SUM(duration_in_second) as call_dial_time_6m\n" +
                    "FROM\n" +
                    "\tcarrier_voicecall\n" +
                    "where \n" +
                    " DATE(date_add(now(), interval -180 day))<\n" +
                    "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                    "and  mobile = '$mobile' " +
        "and task_id = '$taskId' "
        return conn.rxQuery(sql).doAfterTerminate(conn::close)
            .doOnError {
                log.info("近6月主叫通话时长")
                print(it.printStackTrace())
            }
    }

    /**
     *近6月主叫月均通话时长
     */
    fun countLessSixMonthTimeAvg(conn:SQLConnection, mobile: String, taskId: String): Single<ResultSet> {
        log.info("近6月主叫月均通话时长")
        var sql: String =
            "SELECT\n" +
                    "SUM(duration_in_second)/6 as avg_call_dial_time_6m\n" +
                    "FROM\n" +
                    "\tcarrier_voicecall\n" +
                    "where \n" +
                    " DATE(date_add(now(), interval -180 day))<\n" +
                    "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                    "and  mobile = '$mobile' " +
        "and task_id = '$taskId' "
        return conn.rxQuery(sql).doAfterTerminate(conn::close)
            .doOnError {
                log.info("近6月主叫月均通话时长")
                print(it.printStackTrace())
            }
    }

    /**
     *近1月通话次数
     */
    fun countLessOneMonthAll(conn:SQLConnection, mobile: String, taskId: String): Single<ResultSet> {
        log.info("近1月通话次数")
        var sql: String =
            "SELECT\n" +
                    "COUNT(*) as call_cnt_1m \n" +
                    "\n" +
                    "FROM\n" +
                    "\tcarrier_voicecall\n" +
                    "where \n" +
                    " (\n" +
                    "\n" +
                    " DATE(date_add(now(), interval -30 day))<\n" +
                    "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                    "and  \n" +
                    "mobile = '$mobile' \n" +
        "and task_id =  '$taskId'  " +
        "\n" +
                ") or (\n" +
                "\n" +
                " DATE(date_add(now(), interval -30 day))<\n" +
                "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                "and  \n" +
                "\n" +
                "peer_number = '$mobile' \n" +
        "and task_id =  '$taskId'  " +
        ")\n"

        return conn.rxQuery(sql).doAfterTerminate(conn::close)
            .doOnError {
                log.info("近1月通话次数")
                print(it.printStackTrace())
            }
    }

    /**
     *近3月通话次数
     */
    fun countLessThreeMonthAll(conn:SQLConnection, mobile: String, taskId: String): Single<ResultSet> {
        log.info("近3月通话次数")
        var sql: String =
            "SELECT\n" +
                    "COUNT(*) as call_cnt_3m \n" +
                    "\n" +
                    "FROM\n" +
                    "\tcarrier_voicecall\n" +
                    "where \n" +
                    " (\n" +
                    "\n" +
                    " DATE(date_add(now(), interval -90 day))<\n" +
                    "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                    "and  \n" +
                    "mobile = '$mobile'  \n" +
                    "and task_id =  '$taskId' " +
                    "\n" +
                    ") or (\n" +
                    "\n" +
                    " DATE(date_add(now(), interval -90 day))<\n" +
                    "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                    "and  \n" +
                    "\n" +
                    "peer_number = '$mobile'  \n" +
                    "and task_id =  '$taskId'  " +
                    ")\n"

        return conn.rxQuery(sql).doAfterTerminate(conn::close)
            .doOnError {
                log.info("近3月通话次数")
                print(it.printStackTrace())
            }
    }

    /**
     *近3月平均通话次数
     */
    fun countLessThreeMonthAllAvg(conn:SQLConnection, mobile: String, taskId: String): Single<ResultSet> {
        log.info("近3月平均通话次数")
        var sql: String =
            "SELECT\n" +
                    "COUNT(*)/3 as avg_call_cnt_3m \n" +
                    "\n" +
                    "FROM\n" +
                    "\tcarrier_voicecall\n" +
                    "where \n" +
                    " (\n" +
                    "\n" +
                    " DATE(date_add(now(), interval -90 day))<\n" +
                    "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                    "and  \n" +
                    "mobile = '$mobile'  \n" +
                    "and task_id =  '$taskId' " +
                    "\n" +
                    ") or (\n" +
                    "\n" +
                    " DATE(date_add(now(), interval -90 day))<\n" +
                    "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                    "and  \n" +
                    "\n" +
                    "peer_number = '$mobile'  \n" +
                    "and task_id =  '$taskId'  " +
                    ")\n"

        return conn.rxQuery(sql).doAfterTerminate(conn::close)
            .doOnError {
                log.info("近3月平均通话次数")
                print(it.printStackTrace())
            }
    }


    /**
     *近6月通话次数
     */
    fun countLessSixMonthAll(conn:SQLConnection, mobile: String, taskId: String): Single<ResultSet> {
        log.info("近6月通话次数")
        var sql: String =
            "SELECT\n" +
                    "COUNT(*) as call_cnt_6m \n" +
                    "\n" +
                    "FROM\n" +
                    "\tcarrier_voicecall\n" +
                    "where \n" +
                    " (\n" +
                    "\n" +
                    " DATE(date_add(now(), interval -180 day))<\n" +
                    "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                    "and  \n" +
                    "mobile = '$mobile'  \n" +
        "and task_id =  '$taskId' " +
        "\n" +
                ") or (\n" +
                "\n" +
                " DATE(date_add(now(), interval -180 day))<\n" +
                "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                "and  \n" +
                "\n" +
                "peer_number = '$mobile' \n" +
        "and task_id =  '$taskId'  " +
        ")\n"

        return conn.rxQuery(sql).doAfterTerminate(conn::close)
            .doOnError {
                log.info("近6月通话次数")
                print(it.printStackTrace())
            }

    }

    /**
     *近6月平均通话次数
     */
    fun countLessSixMonthAllAvg(conn:SQLConnection, mobile: String, taskId: String): Single<ResultSet> {
        log.info("近6月平均通话次数")
        var sql: String =
            "SELECT\n" +
                    "COUNT(*)/6 as avg_call_cnt_6m \n" +
                    "\n" +
                    "FROM\n" +
                    "\tcarrier_voicecall\n" +
                    "where \n" +
                    " (\n" +
                    "\n" +
                    " DATE(date_add(now(), interval -180 day))<\n" +
                    "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                    "and  \n" +
                    "mobile = '$mobile'  \n" +
                    "and task_id = '$taskId' " +
                    "\n" +
                    ") or (\n" +
                    "\n" +
                    " DATE(date_add(now(), interval -180 day))<\n" +
                    "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                    "and  \n" +
                    "\n" +
                    "peer_number = '$mobile' \n" +
                    "and task_id =  '$taskId'  " +
                    ")\n"

        return conn.rxQuery(sql).doAfterTerminate(conn::close)
            .doOnError {
                log.info("近6月平均通话次数")
                print(it.printStackTrace())
            }

    }

    /**
     *近1月通话时长（秒）
     */
    fun countLessOneMonthAllTime(conn:SQLConnection, mobile: String, taskId: String): Single<ResultSet> {
        log.info("近1月通话时长（秒）")
        var sql: String = "\n" +
                "SELECT\n" +
                "SUM(duration_in_second) as call_time_1m\n" +
                "\n" +
                "FROM\n" +
                "\tcarrier_voicecall\n" +
                "where \n" +
                " (\n" +
                "\n" +
                " DATE(date_add(now(), interval -30 day))<\n" +
                "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                "and  \n" +
                "mobile =  '$mobile'  " +
        "and task_id = '$taskId'  " +
        "\n" +
                ") or (\n" +
                "\n" +
                " DATE(date_add(now(), interval -30 day))<\n" +
                "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                "and  \n" +
                "\n" +
                "peer_number =  '$mobile'  " +
        "and task_id =  '$taskId' " +
        ")\n"

        return conn.rxQuery(sql).doAfterTerminate(conn::close)
            .doOnError {
                log.info("近1月通话时长")
                print(it.printStackTrace())
            }
    }

    /**
     *近3月通话时长（秒）（秒）
     */
    fun countLessThreeMonthAllTime(conn:SQLConnection, mobile: String, taskId: String): Single<ResultSet> {
        log.info("近3月通话时长（秒）（秒）")
        var sql: String = "\n" +
                "SELECT\n" +
                "SUM(duration_in_second) as call_time_3m\n" +
                "\n" +
                "FROM\n" +
                "\tcarrier_voicecall\n" +
                "where \n" +
                " (\n" +
                "\n" +
                " DATE(date_add(now(), interval -90 day))<\n" +
                "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                "and  \n" +
                "mobile = '$mobile'  " +
        "and task_id = '$taskId'  " +
        "\n" +
                ") or (\n" +
                "\n" +
                " DATE(date_add(now(), interval -90 day))<\n" +
                "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                "and  \n" +
                "\n" +
                "peer_number =  '$mobile'  " +
        "and task_id = '$taskId'  " +
        ")\n"

        return conn.rxQuery(sql).doAfterTerminate(conn::close)
            .doOnError {
                log.info("近3月通话时长")
                print(it.printStackTrace())
            }
    }

    /**
     *近3月平均通话时长（秒）
     */
    fun countLessThreeMonthAllTimeAvg(conn:SQLConnection, mobile: String, taskId: String): Single<ResultSet> {
        log.info("近3月平均通话时长（秒）")
        var sql: String = "\n" +
                "SELECT\n" +
                "SUM(duration_in_second) as avg_call_time_3m\n" +
                "\n" +
                "FROM\n" +
                "\tcarrier_voicecall\n" +
                "where \n" +
                " (\n" +
                "\n" +
                " DATE(date_add(now(), interval -90 day))<\n" +
                "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                "and  \n" +
                "mobile = '$mobile'  " +
                "and task_id = '$taskId'  " +
                "\n" +
                ") or (\n" +
                "\n" +
                " DATE(date_add(now(), interval -90 day))<\n" +
                "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                "and  \n" +
                "\n" +
                "peer_number =  '$mobile'  " +
                "and task_id = '$taskId'  " +
                ")\n"

        return conn.rxQuery(sql).doAfterTerminate(conn::close)
            .doOnError {
                log.info("近3月平均通话时长")
                print(it.printStackTrace())
            }
    }

    /**
     *近6月通话时长（秒）（秒）（秒）
     */
    fun countLessSixMonthAllTime(conn:SQLConnection, mobile: String, taskId: String): Single<ResultSet> {
        log.info("近6月通话时长（秒）（秒）（秒）")
        var sql: String = "\n" +
                "SELECT\n" +
                "SUM(duration_in_second) as call_time_6m\n" +
                "\n" +
                "FROM\n" +
                "\tcarrier_voicecall\n" +
                "where \n" +
                " (\n" +
                "\n" +
                " DATE(date_add(now(), interval -180 day))<\n" +
                "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                "and  \n" +
                "mobile = '$mobile'  " +
        "and task_id = '$taskId'   " +
        "\n" +
                ") or (\n" +
                "\n" +
                " DATE(date_add(now(), interval -180 day))<\n" +
                "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                "and  \n" +
                "\n" +
                "peer_number = '$mobile' " +
        "and task_id =  '$taskId'  "  +
        ")\n"

        return conn.rxQuery(sql).doAfterTerminate(conn::close)
            .doOnError {
            log.info("近6月通话时长（秒）（秒）（秒")
            print(it.printStackTrace())
        }
    }

    /**
     *近6月平均通话时长（秒）
     */
    fun countLessSixMonthAllTimeAvg(conn:SQLConnection, mobile: String, taskId: String): Single<ResultSet> {
        log.info("近6月平均通话时长（秒）")
        var sql: String = "\n" +
                "SELECT\n" +
                "SUM(duration_in_second) as call_time_6m\n" +
                "\n" +
                "FROM\n" +
                "\tcarrier_voicecall\n" +
                "where \n" +
                " (\n" +
                "\n" +
                " DATE(date_add(now(), interval -180 day))<\n" +
                "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                "and  \n" +
                "mobile = '$mobile'  " +
                "and task_id = '$taskId'   " +
                "\n" +
                ") or (\n" +
                "\n" +
                " DATE(date_add(now(), interval -180 day))<\n" +
                "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                "and  \n" +
                "\n" +
                "peer_number = '$mobile' " +
                "and task_id =  '$taskId'  "  +
                ")\n"

        return conn.rxQuery(sql).doAfterTerminate(conn::close)
            .doOnError {
            log.info("近6月平均通话时长（秒）")
            print(it.printStackTrace())
        }
    }


}