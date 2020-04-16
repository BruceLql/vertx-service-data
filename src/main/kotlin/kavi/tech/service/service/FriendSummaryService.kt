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
 * @date 2020/4/13 17:56
 * @Description
 * <a href="goodmanalibaba@foxmail.com"></a>
 * @Versin 1.0
 */
@Service
class FriendSummaryService {
    @Autowired
    private lateinit var carrierResultDataDao: CarrierResultDataDao

    @Autowired
    private lateinit var client: AsyncSQLClient

    /**
     * 获取统计数据 朋友圈联系人数量（friend_circle.summary）
     */
    fun toCleaningCircleFriendsData(mobile: String, task_id: String): Single<JsonObject> {
        if (StringUtils.isBlank(mobile) || StringUtils.isBlank(task_id)) {
            throw IllegalAccessException("数据为空！")
        }
        var json = JsonObject()
        return this.client.rxGetConnection().flatMap { conn ->
            Observable.concat(
                listOf(
                    countLess3Months(conn, mobile, task_id).toObservable(),
                    countLess3MonthsGoupy(conn, mobile, task_id).toObservable(),
                    countLess3Attribution(conn, mobile, task_id).toObservable(),
                    attributionMobilePhoneNumber(conn, mobile, task_id).toObservable(),
                    contactPerson(conn, mobile, task_id).toObservable(),
                    contactPersonLessSixMonth(conn, mobile, task_id).toObservable(),
                    contactPersonTen(conn, mobile, task_id).toObservable(),
                    contactPersonTenHomeArea(conn, mobile, task_id).toObservable(),
                    attributionMobilePhoneNumberHun(conn, mobile, task_id).toObservable(),
                    contactPersonHun(conn, mobile, task_id).toObservable()
                )
            ).toList().toSingle().doAfterTerminate(conn::close)
        }
            .map {
                json.put("friend_num_3m",if (it[0].rows.size==0) "0" else it[0].rows[0].getValue("friend_num_3m").toString())
                json.put("good_friend_num_3m",if (it[1].rows.size==0) "0" else it[1].rows[0].getValue("good_friend_num_3m").toString() )
                json.put("friend_city_center_3m", if (it[2].rows.size==0) "0" else it[2].rows[0].getValue("friend_city_center_3m").toString())
                json.put("is_city_match_friend_city_center_3m",if (it[3].rows.size==0) "0" else it[3].rows[0].getValue("is_city_match_friend_city_center_3m").toString() )
                json.put("inter_peer_num_3m", if (it[4].rows.size==0) "0" else it[4].rows[0].getValue("inter_peer_num_3m").toString())
                json.put("friend_num_6m", if (it[5].rows.size==0) "0" else it[5].rows[0].getValue("friend_num_6m").toString())
                json.put("good_friend_num_6m", if (it[6].rows.size==0) "0" else it[6].rows[0].getValue("good_friend_num_6m").toString())
                json.put("friend_city_center_6m", if (it[7].rows.size==0) "0" else it[7].rows[0].getValue("friend_city_center_6m").toString())
                json.put("is_city_match_friend_city_center_6m", if (it[8].rows.size==0) "0" else it[8].rows[0].getValue("is_city_match_friend_city_center_6m").toString())
                json.put("inter_peer_num_6m", if (it[9].rows.size==0) "0" else it[9].rows[0].getValue("inter_peer_num_6m").toString())
        }
    }


    /**
     * 统计 近90天月联系人数量（去重）(0-90天)
     */
    fun countLess3Months(conn:SQLConnection, mobile: String, taskId: String): Single<ResultSet> {
        log.info("统计 近90天月联系人数量（去重）(0-90天)")
        var sql: String = "SELECT \n" +
                "\t  IFNULL(COUNT(DISTINCT peer_number),0)  as \"friend_num_3m\"\n" +
                "\t\tFROM\n" +
                "\t\tcarrier_voicecall\n" +
                "\t\twhere \n" +
                "\t\t DATE(date_add(now(), interval -90 day))<\n" +
                "\t\tDATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                "\t\tand  mobile =  '$mobile'\n" +
                "and task_id = '$taskId' "
        return conn.rxQuery(sql)
    }

    /**
     * 近90天联系人数量（联系10次以上，去重）（0-90天）
     */
    fun countLess3MonthsGoupy(conn:SQLConnection, mobile: String, taskId: String): Single<ResultSet> {
        log.info("近90天联系人数量（联系10次以上，去重） ")
        var sql: String = "SELECT\n" +
                "\t  IFNULL(COUNT(DISTINCT peer_number),0)  as good_friend_num_3m\n" +
                "FROM\n" +
                "\tcarrier_voicecall\n" +
                "WHERE\n" +
                "\tDATE(\n" +
                "\t\tdate_add(now(), INTERVAL - 90 DAY)\n" +
                "\t) < DATE(\n" +
                "\t\tCONCAT(\n" +
                "\t\t\tSUBSTR(bill_month, 1, 4),\n" +
                "\t\t\t\"-\",\n" +
                "\t\t\ttime\n" +
                "\t\t)\n" +
                "\t)\n" +
                "AND mobile = '$mobile'" +
                "AND task_id =  '$taskId'" +
                "GROUP BY\n" +
                "\tpeer_number\n" +
                "HAVING\n" +
                "\t(COUNT(peer_number) > 10) limit 1"

        return conn.rxQuery(sql)
    }

    /***
     * 近90天联系次数最多的号码归属地（0-90天）
     */
    var sql: String = ""

    fun countLess3Attribution(conn:SQLConnection, mobile: String, taskId: String): Single<ResultSet> {
        log.info("近90天联系次数最多的号码归属地（0-90天）")
        var sql: String = "SELECT friend_city_center_3m from ( \n" +
                "\tSELECT \n" +
                "\tIF(homearea is null or homearea =\"\",\"未匹配\", homearea) as friend_city_center_3m, \n" +
                "\tCOUNT(homearea) as \"countTime\"\n" +
                "\tfrom carrier_voicecall \n" +
                "\twhere \n" +
                "\tmobile =  '$mobile'\n" +
                "and task_id =  \"'$taskId'\"\n" +
                "and DATE(date_add(now(), interval - 90 day))<\n" +
                "\tDATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                "\tGROUP BY homearea \n" +
                "\tunion all \n" +
                "\tSELECT \n" +
                "\tIF(location is null or location =\"\",\"未匹配\", location) as friend_city_center_3m,\n" +
                "\tCOUNT(location) as \"countTime\"\n" +
                "\tfrom carrier_voicecall \n" +
                "\twhere \n" +
                "mobile =  '$mobile'\n" +
                "and task_id =  \"$taskId\"\n" +
                "and DATE(date_add(now(), interval - 90 day))<\n" +
                "\tDATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time)) \n" +
                "\tGROUP BY location  \n" +
                " ) as c \n" +
                "\tHAVING(MAX(countTime))"
        return conn.rxQuery(sql)
    }

    /***
     * 近90天朋友圈中心城市是否与手机归属地一致（0-90天）
     */
    fun attributionMobilePhoneNumber(conn:SQLConnection, mobile: String, taskId: String): Single<ResultSet> {
        log.info("近90天朋友圈中心城市是否与手机归属地一致（0-90天）")
        var sql: String = "SELECT \n" +
                "  (\n" +
                "\tCASE\n" +
                "\tWHEN carrier_baseinfo.city is null THEN FALSE\n" +
                "\tWHEN c.homearea is null THEN FALSE\n" +
                "\tWHEN carrier_baseinfo.city  <=> c.homearea  THEN true\n" +
                "\tELSE FALSE \n" +
                "\tEND\n" +
                "\t)\n" +
                "\tas is_city_match_friend_city_center_3m\n" +
                "\tfrom  (\n" +
                "  SELECT MAX(is_city_match_friend_city_center_3m) as is_city_match_friend_city_center_3m,peer_number,homearea from (\n" +
                "  \tSELECT\n" +
                "\tpeer_number as peer_number,\n" +
                "COUNT(peer_number) as \"is_city_match_friend_city_center_3m\",\n" +
                "homearea\n" +
                "\tFROM\n" +
                "\tcarrier_voicecall\n" +
                "\twhere \n" +
                "\tDATE(date_add(now(), interval -90 day))<\n" +
                "\t DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                "\tand carrier_voicecall.mobile = '$mobile' " +
                "\tand carrier_voicecall.task_id =  '$taskId' " +
                "GROUP BY peer_number\n" +
                "  ) as bb\n" +
                ") as c LEFT JOIN carrier_baseinfo on carrier_baseinfo.mobile = c.peer_number"

        return conn.rxQuery(sql)
    }

    /***
     *  近90天互有主叫和被叫的联系人电话号码数目（去重）（0-90天）
     */
    fun contactPerson(conn:SQLConnection, mobile: String, taskId: String): Single<ResultSet> {
        log.info("近90天互有主叫和被叫的联系人电话号码数目（去重）（0-90天）")
        var sql: String = "SELECT IFNULL( COUNT(*) ,0) as \"inter_peer_num_3m\"\n" +
                "from ( \n" +
                "SELECT \n" +
                "DISTINCT peer_number \n" +
                "from carrier_voicecall\n" +
                " where DATE(date_add(now(), interval -90 day))<\n" +
                " DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                "and mobile =  '$mobile' " +
                "and task_id =  '$taskId' " +
                "\n" +
                "union \n" +
                "\n" +
                "SELECT\n" +
                " DISTINCT carrier_voicecall.mobile\n" +
                " from carrier_voicecall \n" +
                "where \n" +
                "carrier_voicecall.peer_number = '$mobile'" +
                "and carrier_voicecall.task_id = '$taskId' " +
                "\t\t\t\t and  DATE(date_add(now(), interval -90 day))<\n" +
                "\t\t\t\t DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                ") as b \n"
        return conn.rxQuery(sql)
    }


    /***
     *  近180天的联系人数量
     */
    fun contactPersonLessSixMonth(conn:SQLConnection, mobile: String, taskId: String): Single<ResultSet> {
        log.info("近180天的联系人数量")
        var sql: String = "SELECT \n" +
                "\t  IFNULL(COUNT(DISTINCT peer_number),0)  as \"friend_num_6m\"\n" +
                "\t\tFROM\n" +
                "\t\tcarrier_voicecall\n" +
                "\t\twhere \n" +
                "\t\t DATE(date_add(now(), interval -180 day))<\n" +
                "\t\tDATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                "\t\tand  mobile =  '$mobile' \n" +
                "and task_id = '$taskId'  "

        return conn.rxQuery(sql)
    }

    /***
     *  近180天的联系人数量（联系10次以上，去重）（0-180天）
     */
    fun contactPersonTen(conn:SQLConnection, mobile: String, taskId: String): Single<ResultSet> {
        log.info("近180天的联系人数量（联系10次以上，去重）（0-180天）")
        var sql: String = "SELECT\n" +
                "\t IFNULL(COUNT(DISTINCT peer_number),0)  as good_friend_num_6m\n" +
                "FROM\n" +
                "\tcarrier_voicecall\n" +
                "WHERE\n" +
                "\tDATE(\n" +
                "\t\tdate_add(now(), INTERVAL - 180 DAY)\n" +
                "\t) < DATE(\n" +
                "\t\tCONCAT(\n" +
                "\t\t\tSUBSTR(bill_month, 1, 4),\n" +
                "\t\t\t\"-\",\n" +
                "\t\t\ttime\n" +
                "\t\t)\n" +
                "\t)\n" +
                "AND mobile = '$mobile'  " +
                "AND task_id = '$taskId'  " +
                "GROUP BY\n" +
                "\tpeer_number\n" +
                "HAVING\n" +
                "\t(COUNT(peer_number) > 10) limit 1"
        return conn.rxQuery(sql)
    }

    /***
     * 近180天的联系次数最多的号码归属地（0-180天）
     */
    fun contactPersonTenHomeArea(conn:SQLConnection, mobile: String, taskId: String): Single<ResultSet> {
        log.info("近180天的联系次数最多的号码归属地（0-180天）")
        var sql: String = "SELECT friend_city_center_6m from (\n" +
                "\n" +
                "SELECT \n" +
                "IF(homearea is null or homearea =\"\",\"未匹配\", homearea) as friend_city_center_6m,\n" +
                "COUNT(homearea) as \"countTime\"\n" +
                "\n" +
                "from carrier_voicecall \n" +
                "where \n" +
                " mobile = '$mobile'  " +
                "and task_id = '$mobile'  " +
                "and DATE(date_add(now(), interval - 180 day))<\n" +
                "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                "GROUP BY homearea \n" +
                "\n" +
                "union all \n" +
                "\n" +
                "SELECT \n" +
                "IF(location is null or location =\"\",\"未匹配\", location) as friend_city_center_6m,\n" +
                "COUNT(location) as \"countTime\"\n" +
                "\n" +
                "from carrier_voicecall \n" +
                "where \n" +
                " peer_number = '$mobile'  " +
                "and task_id ='$taskId'   " +
                "and DATE(date_add(now(), interval - 180 day))<\n" +
                "DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                "GROUP BY location \n" +
                "\n" +
                "\n" +
                ") as c \n" +
                "HAVING(MAX(countTime))"
        return conn.rxQuery(sql)
    }

    /***
     * 近180天的朋友圈中心城市是否与手机归属地一致（0-180天）
     */
    fun attributionMobilePhoneNumberHun(conn:SQLConnection, mobile: String, taskId: String): Single<ResultSet> {
        log.info("近180天的朋友圈中心城市是否与手机归属地一致（0-180天）")
        var sql: String = "SELECT \n" +
                "  (\n" +
                "\tCASE\n" +
                "\tWHEN carrier_baseinfo.city is null THEN FALSE\n" +
                "\tWHEN c.homearea is null THEN FALSE\n" +
                "\tWHEN carrier_baseinfo.city  <=> c.homearea  THEN true\n" +
                "\tELSE FALSE \n" +
                "\tEND\n" +
                "\t)\n" +
                "\tas is_city_match_friend_city_center_6m\n" +
                "\tfrom  (\n" +
                "  SELECT MAX(countTime) as countTime,peer_number,homearea from (\n" +
                "  \tSELECT\n" +
                "\tpeer_number as peer_number,\n" +
                "COUNT(peer_number) as \"countTime\",\n" +
                "homearea\n" +
                "\tFROM\n" +
                "\tcarrier_voicecall\n" +
                "\twhere \n" +
                "\tDATE(date_add(now(), interval -180 day))<\n" +
                "\t DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                "\tand carrier_voicecall.mobile = '$mobile'  " +
                "\tand carrier_voicecall.task_id =  '$taskId'  " +
                "GROUP BY peer_number\n" +
                "  ) as bb\n" +
                ") as c LEFT JOIN carrier_baseinfo on carrier_baseinfo.mobile = c.peer_number"

        return conn.rxQuery(sql)

    }

    /***
     *  近180天的互有主叫和被叫的联系人电话号码数目（去重）（0-180天）
     */
    fun contactPersonHun(conn:SQLConnection,mobile: String, taskId: String): Single<ResultSet> {
        log.info("近180天的互有主叫和被叫的联系人电话号码数目（去重）（0-180天）")
        var sql: String = "SELECT IFNULL(COUNT(*),0) as \"inter_peer_num_6m\"\n" +
                "from ( \n" +
                "SELECT \n" +
                "DISTINCT peer_number \n" +
                "from carrier_voicecall\n" +
                " where DATE(date_add(now(), interval -180 day))<\n" +
                " DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                "and mobile = '$mobile'  " +
                "and task_id = '$taskId'   " +
                "\n" +
                "union \n" +
                "\n" +
                "SELECT\n" +
                " DISTINCT carrier_voicecall.mobile\n" +
                " from carrier_voicecall \n" +
                "where \n" +
                "carrier_voicecall.peer_number = '$mobile'  " +
                "and carrier_voicecall.task_id = '$taskId'  " +
                "\t\t\t\t and  DATE(date_add(now(), interval -180 day))<\n" +
                "\t\t\t\t DATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                ") as b \n"

        return conn.rxQuery(sql)
//        return carrierResultDataDao.customizeSQL(sql)
//            .map {
//                it[0]?.getInteger("countTime")
//            }.doOnError {
//                log.info("近180天的互有主叫和被叫的联系人电话号码数目（去重）（0-180天）")
//                print(it.printStackTrace())
//            }
    }
}
