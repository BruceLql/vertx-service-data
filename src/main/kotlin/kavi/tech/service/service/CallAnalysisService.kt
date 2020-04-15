package kavi.tech.service.service

import io.vertx.codegen.CodeGenProcessor.log
import io.vertx.core.json.JsonObject
import io.vertx.ext.sql.ResultSet
import io.vertx.rxjava.ext.asyncsql.AsyncSQLClient
import io.vertx.rxjava.ext.sql.SQLConnection
import kavi.tech.service.mysql.dao.CarrierResultDataDao
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

    /**
     * 通话风险分析（call_risk_analysis）
     *
     */
    fun toCleaningCircleFriendsData(mobile: String, task_id: String): Single<JsonObject> {
        if (StringUtils.isBlank(mobile) || StringUtils.isBlank(task_id)) {
            throw IllegalAccessException("数据为空！")
        }
        var jsonObject = JsonObject()
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
                    becountLessSixMonthAvg(conn,mobile,task_id).toObservable(),
                    LastOneMonthCallTime(conn,mobile,task_id).toObservable(),
                    LastThreeMonthCallTime(conn,mobile,task_id).toObservable(),
                    LastSixMonthCallTime(conn,mobile,task_id).toObservable(),
                    avgLastThreeMonthCallTime(conn,mobile,task_id).toObservable(),
                    avgLastSixMonthCallTime(conn,mobile,task_id).toObservable()
                )
            ).toList().toSingle()
        }.map {

            jsonObject.put("call_cnt_1m", if (it[0].rows.size==0) 0 else it[0].rows[0].getValue("call_cnt_1m")) // 近1月通话次数
            jsonObject.put("call_cnt_3m", if (it[1].rows.size==0) 0 else it[1].rows[0].getValue("call_cnt_3m"))  // 近3月通话次数
            jsonObject.put("call_cnt_6m", if (it[2].rows.size==0) 0 else it[2].rows[0].getValue("call_cnt_6m")) //近6月通话次数
            jsonObject.put("avg_call_cnt_3m", if (it[3].rows.size==0) 0 else it[3].rows[0].getString("avg_call_cnt_3m").toFloat()) // 近3月平均通话次数
            jsonObject.put("avg_call_cnt_6m", if (it[4].rows.size==0) 0 else it[4].rows[0].getString("avg_call_cnt_6m").toFloat()) // 近6月平均通话次数
            jsonObject.put("call_time_1m", if (it[5].rows.size==0) 0 else it[5].rows[0].getString("call_time_1m").toLong()) // 近1月通话时长（秒）
            jsonObject.put("call_time_3m", if (it[6].rows.size==0) 0 else it[6].rows[0].getString("call_time_3m").toLong()) // 近3月通话时长（秒）
            jsonObject.put("call_time_6m", if (it[7].rows.size==0) 0 else it[7].rows[0].getString("call_time_6m").toLong()) // 近6月通话时长
            jsonObject.put("avg_call_time_3m", if (it[8].rows.size==0) 0 else it[8].rows[0].getString("avg_call_time_3m").toFloat()) // 近3月平均通话时长
            jsonObject.put("avg_call_time_6m", if (it[9].rows.size==0) 0 else it[9].rows[0].getString("avg_call_time_6m").toFloat()) //近6月平均通话时长
            jsonObject.put("call_dial_cnt_1m", if (it[10].rows.size==0) 0 else it[10].rows[0].getValue("call_dial_cnt_1m")) // 近1月主叫通话次数
            jsonObject.put("call_dial_cnt_3m", if (it[11].rows.size==0) 0 else it[11].rows[0].getValue("call_dial_cnt_3m"))//近3月主叫通话次数
            jsonObject.put("call_dial_cnt_6m", if (it[12].rows.size==0) 0 else it[12].rows[0].getValue("call_dial_cnt_6m"))//近6月主叫通话次数
            jsonObject.put("avg_call_dial_cnt_3m", if (it[13].rows.size==0) 0 else it[13].rows[0].getValue("avg_call_dial_cnt_3m"))//近3月主叫月均通话次数
            jsonObject.put("avg_call_dial_cnt_6m", if (it[14].rows.size==0) 0 else it[14].rows[0].getValue("avg_call_dial_cnt_6m")) //近6月主叫月均通话次数
            jsonObject.put("call_dial_time_1m", if (it[15].rows.size==0) 0 else it[15].rows[0].getString("call_dial_time_1m").toLong())//近1月主叫通话时长
            jsonObject.put("call_dial_time_3m", if (it[16].rows.size==0) 0 else it[16].rows[0].getString("call_dial_time_3m").toLong())//近3月主叫通话时长
            jsonObject.put("call_dial_time_6m", if (it[17].rows.size==0) 0 else it[17].rows[0].getString("call_dial_time_6m").toLong())//近6月主叫通话时长
            jsonObject.put("avg_call_dial_time_3m", if (it[18].rows.size==0) 0 else it[18].rows[0].getString("avg_call_dial_time_3m").toFloat())//近3月主叫月均通话时长
            jsonObject.put("avg_call_dial_time_6m", if (it[19].rows.size==0) 0 else it[19].rows[0].getString("avg_call_dial_time_6m").toFloat())//近6月主叫月均通话时长
            jsonObject.put("call_dialed_cnt_1m", if (it[20].rows.size==0) 0 else it[20].rows[0].getValue("call_dialed_cnt_1m"))//近1个月被叫通话次数
            jsonObject.put("call_dialed_cnt_3m", if (it[21].rows.size==0) 0 else it[21].rows[0].getValue("call_dialed_cnt_3m"))//近3个月被叫通话次数
            jsonObject.put("call_dialed_cnt_6m", if (it[22].rows.size==0) 0 else it[22].rows[0].getValue("call_dialed_cnt_6m"))//近6个月被叫通话次数
            jsonObject.put("avg_call_dialed_cnt_3m", if (it[23].rows.size==0) 0 else it[23].rows[0].getValue("avg_call_dialed_cnt_3m"))//近3月被叫月均通话次数

            jsonObject.put("avg_call_dialed_cnt_6m", if (it[24].rows.size==0) 0 else it[24].rows[0].getValue("avg_call_dialed_cnt_6m"))//近6月被叫月均通话次数

            jsonObject.put("call_dialed_time_1m", if (it[25].rows.size==0) 0 else it[25].rows[0].getValue("call_dialed_time_1m"))//近1月被叫通话时长
            jsonObject.put("call_dialed_time_3m", if (it[26].rows.size==0) 0 else it[26].rows[0].getValue("call_dialed_time_3m"))//近3月被叫通话时长
            jsonObject.put("call_dialed_time_6m", if (it[27].rows.size==0) 0 else it[27].rows[0].getValue("call_dialed_time_6m"))//近6月被叫通话时长
            jsonObject.put("avg_call_dialed_time_3m", if (it[28].rows.size==0) 0 else it[28].rows[0].getString("avg_call_dialed_time_3m").toFloat())//近3月被叫月均通话时长
            jsonObject.put("avg_call_dialed_time_6m", if (it[29].rows.size==0) 0 else it[29].rows[0].getString("avg_call_dialed_time_6m").toFloat())//近6月被叫月均通话时长

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
                    " IFNULL(COUNT(id),0)  as call_dial_cnt_1m \n" +
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
     * 近1月被叫通话时长
     *
     */
    fun LastOneMonthCallTime(conn: SQLConnection, mobile: String, taskId: String): Single<ResultSet> {
        log.info("近一个月被叫通话时长")
        var sql:String = " select count(if(cv.dial_type='DIALED',true,null)) as call_dialed_time_1m from carrier_voicecall cv where cv.mobile = '$mobile' and cv.task_id = '$taskId' and cv.time between DATE_FORMAT(DATE(date_add(now(), interval -1 month)),'%m-%d %H:%i:%s') and date_format(now(),'%m-%d %H:%i:%s');"
        return conn.rxQuery(sql).doAfterTerminate(conn::close)
            .doOnError {
                log.info("近一个月被叫通话时长")
                print(it.printStackTrace())
            }
    }
    /***
     *
     * 近三个月被叫通话时长
     *
     */
    fun LastThreeMonthCallTime(conn: SQLConnection, mobile: String, taskId: String): Single<ResultSet> {
        log.info("近三个月被叫通话时长")
        var sql:String = " select count(if(cv.dial_type='DIALED',true,null)) as call_dialed_time_3m from carrier_voicecall cv where cv.mobile = '$mobile' and cv.task_id = '$taskId' and cv.time between DATE_FORMAT(DATE(date_add(now(), interval -3 month)),'%m-%d %H:%i:%s') and date_format(now(),'%m-%d %H:%i:%s');"
        return conn.rxQuery(sql).doAfterTerminate(conn::close)
            .doOnError {
                log.info("近三个月被叫通话时长")
                print(it.printStackTrace())
            }
    }

    /***
     *
     * 近3月被叫月均通话时长
     *
     */
    fun avgLastThreeMonthCallTime(conn: SQLConnection, mobile: String, taskId: String): Single<ResultSet> {
        log.info("近3月被叫月均通话时长")
        var sql:String = " select count(if(cv.dial_type='DIALED',true,null))/3 as avg_call_dialed_time_3m from carrier_voicecall cv where cv.mobile = '$mobile' and cv.task_id = '$taskId' and cv.time between DATE_FORMAT(DATE(date_add(now(), interval -3 month)),'%m-%d %H:%i:%s') and date_format(now(),'%m-%d %H:%i:%s');"
        return conn.rxQuery(sql).doAfterTerminate(conn::close)
            .doOnError {
                log.info("近3月被叫月均通话时长")
                print(it.printStackTrace())
            }
    }
    /***
     *
     * 近六个月被叫通话时长
     *
     */
    fun LastSixMonthCallTime(conn: SQLConnection, mobile: String, taskId: String): Single<ResultSet> {
        log.info("近六个月被叫通话时长")
        var sql:String = " select count(if(cv.dial_type='DIALED',true,null)) as call_dialed_time_6m from carrier_voicecall cv where cv.mobile = '$mobile' and cv.task_id = '$taskId' and cv.time between DATE_FORMAT(DATE(date_add(now(), interval -6 month)),'%m-%d %H:%i:%s') and date_format(now(),'%m-%d %H:%i:%s');"
        return conn.rxQuery(sql).doAfterTerminate(conn::close)
            .doOnError {
                log.info("近六个月被叫通话时长")
                print(it.printStackTrace())
            }
    }

    /***
     *
     * 近6月被叫月均通话时长
     *
     */
    fun avgLastSixMonthCallTime(conn: SQLConnection, mobile: String, taskId: String): Single<ResultSet> {
        log.info("近6月被叫月均通话时长")
        var sql:String = " select count(if(cv.dial_type='DIALED',true,null))/6 as avg_call_dialed_time_6m from carrier_voicecall cv where cv.mobile = '$mobile' and cv.task_id = '$taskId' and cv.time between DATE_FORMAT(DATE(date_add(now(), interval -6 month)),'%m-%d %H:%i:%s') and date_format(now(),'%m-%d %H:%i:%s');"
        return conn.rxQuery(sql).doAfterTerminate(conn::close)
            .doOnError {
                log.info("近6月被叫月均通话时长")
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
                    " IFNULL(COUNT(id),0) as call_dialed_cnt_1m \n" +
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
                    "  IFNULL(COUNT(id),0)  as avg_call_dial_cnt_3m\n" +
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
                    "  IFNULL(COUNT(id),0)  as call_dial_cnt_3m\n" +
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
                    " IFNULL(COUNT(id),0)  as call_dialed_cnt_3m\n" +
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
                    " IFNULL(COUNT(id),0) as avg_call_dialed_cnt_3m\n" +
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
                    " IFNULL(COUNT(id),0)  AS call_dialed_cnt_6m\n" +
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
        var sql: String = "SELECT IFNULL(COUNT(id),0)  AS avg_call_dialed_cnt_6m\tFROM \tcarrier_voicecall\twhere \tDATE(date_add(now(), interval -180 day))<\tDATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\tand  peer_number = '$mobile'\tand task_id = '$taskId'"

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
                    "  IFNULL(COUNT(id),0)  as call_dial_cnt_6m\n" +
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
                    "  IFNULL(COUNT(id),0)  as avg_call_dial_cnt_6m\n" +
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
                    "  IFNULL(SUM(duration_in_second),0)  as call_dial_time_1m\n" +
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
                    " IFNULL(SUM(duration_in_second),0)  as call_dial_time_3m\n" +
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
                    "IFNULL(SUM(duration_in_second),0)/3  as avg_call_dial_time_3m\n" +
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
                    "  IFNULL(SUM(duration_in_second),0)  as call_dial_time_6m\n" +
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
                    "IFNULL(SUM(duration_in_second),0)/6  as avg_call_dial_time_6m\n" +
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
                    "IFNULL(COUNT(*),0)  as call_cnt_1m \n" +
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
                    " IFNULL(COUNT(*),0)  as call_cnt_3m \n" +
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
                    " IFNULL(COUNT(*),0) /3 as avg_call_cnt_3m \n" +
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
                    "  IFNULL(COUNT(*),0)  as call_cnt_6m \n" +
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
                    "  IFNULL(COUNT(*),0) /6 as avg_call_cnt_6m \n" +
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
                " IFNULL(SUM(duration_in_second),0)  as call_time_1m\n" +
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
                " IFNULL(SUM(duration_in_second),0) as call_time_3m\n" +
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
                " IFNULL(SUM(duration_in_second),0)/3 as avg_call_time_3m\n" +
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
                " IFNULL(SUM(duration_in_second),0)  as call_time_6m\n" +
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
                " IFNULL(SUM(duration_in_second),0)/6 as avg_call_time_6m\n" +
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