package kavi.tech.service.service

import io.vertx.codegen.CodeGenProcessor.log
import io.vertx.core.json.Json
import io.vertx.core.json.JsonObject
import io.vertx.rxjava.ext.asyncsql.AsyncSQLClient
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


    fun toCleaningCircleFriendsData(mobile: String, task_id: String): Single<Unit>? {
        if (StringUtils.isBlank(mobile) || StringUtils.isBlank(task_id)) {
            throw IllegalAccessException("数据为空！")
        }

        val jsonResultDataList = ArrayList<JsonObject>()
        val carrierResultDataList = ArrayList<CarrierResultData>()
        val carrierResultData = CarrierResultData()

        carrierResultData.mobile = mobile
        carrierResultData.task_id = task_id

        log.info("查询数据逻辑real 开始=======")
        var countLess3Months: Single<Int> = countLess3Months(mobile, task_id)//统计 近90天月联系人数量（去重）(0-90天)
        var countLess3MonthsGoupy: Single<Int> = countLess3MonthsGoupy(mobile, task_id)//近90天联系人数量（联系10次以上，去重）（0-90天）
        var countLess3Attribution: Single<String> = countLess3Attribution(mobile, task_id)//近90天联系次数最多的号码归属地（0-90天）
        var attributionMobilePhoneNumber: Single<Long> =
            attributionMobilePhoneNumber(mobile, task_id)//近90天朋友圈中心城市是否与手机归属地一致（0-90天）
        var contactPerson: Single<Int> = contactPerson(mobile, task_id)//近90天互有主叫和被叫的联系人电话号码数目（去重）
        var contactPersonLessSixMonth: Single<Int> = contactPersonLessSixMonth(mobile, task_id)//近180天的联系人数量重）（0-180天）
        var contactPersonTen: Single<Int> = contactPersonTen(mobile, task_id)//近180天的联系人数量（联系10次以上，去重）（0-180天）
        var contactPersonTenHomeArea: Single<String> =
            contactPersonTenHomeArea(mobile, task_id)//近180天的联系次数最多的号码归属地（0-180天）
        var attributionMobilePhoneNumberHun: Single<Long> =
            attributionMobilePhoneNumberHun(mobile, task_id)//近180天的朋友圈中心城市是否与手机归属地一致（0-180天）
        var contactPersonHun: Single<Int> = contactPersonHun(mobile, task_id)//近180天的互有主叫和被叫的联系人电话号码数目（去重）（0-180天）
        log.info("查询数据逻辑开始=======")
        var list = assemblyParametersList(
            countLess3Months,
            countLess3MonthsGoupy,
            countLess3Attribution,
            attributionMobilePhoneNumber,
            contactPerson,
            contactPersonLessSixMonth,
            contactPersonTen,
            contactPersonTenHomeArea,
            attributionMobilePhoneNumberHun,
            contactPersonHun
        )

//                        return Single.just(carrierResultDataList.add(
//                            CarrierResultData()
//
//                        ))
        //TODO 需要梳理逻辑
        return Observable.concat(list).toList()
            .doOnError {
                print(it.printStackTrace())
            }
            .map { it ->
                var jsonObject: JsonObject = JsonObject()
                jsonObject.put("friend_num_3m", it[0].getInteger("friend_num_3m"))
                jsonObject.put("good_friend_num_3m", it[1].getInteger("good_friend_num_3m"))
                jsonObject.put("friend_city_center_3m", it[2].getString("friend_city_center_3m"))
                jsonObject.put(
                    "is_city_match_friend_city_center_3m",
                    it[3].getLong("is_city_match_friend_city_center_3m")
                )
                jsonObject.put("inter_peer_num_3m", it[4].getInteger("inter_peer_num_3m"))
                jsonObject.put("friend_num_6m", it[5].getInteger("friend_num_6m"))
                jsonObject.put("good_friend_num_6m", it[6].getInteger("good_friend_num_6m"))
                jsonObject.put("friend_city_center_6m", it[7].getString("friend_city_center_6m"))
                jsonObject.put(
                    "is_city_match_friend_city_center_6m",
                    it[8].getLong("is_city_match_friend_city_center_6m")
                )
                jsonObject.put("inter_peer_num_6m", it[9].getInteger("inter_peer_num_6m"))
                jsonResultDataList.add(jsonObject)
                log.info("查询数据逻辑结束=======001" + Json.encode(jsonResultDataList))
                log.info("查询数据逻辑结束=======" + jsonResultDataList.size)
                log.info("查询数据逻辑结束=======" + Json.encode(jsonResultDataList))

                carrierResultData.task_id = task_id
                carrierResultData.mobile = mobile
                carrierResultData.item = "friend_circle"
                carrierResultData.result = jsonObject.toString()
                carrierResultDataList.add(carrierResultData)

                log.info("查询数据逻辑结束=======" + carrierResultDataList.size)
                log.info("查询数据逻辑结束=======" + Json.encode(carrierResultDataList))
            }
            .doOnError {
                print(it.printStackTrace())
            }.toSingle()

        log.info("执行return ")

        /*                    .subscribe { it->

                                var jsonObject: JsonObject = JsonObject()
                                jsonObject.put("friend_num_3m", it[0].getInteger("friend_num_3m"))
                                jsonObject.put("good_friend_num_3m", it[1].getInteger("good_friend_num_3m"))
                                jsonObject.put("friend_city_center_3m", it[2].getString("friend_city_center_3m"))
                                jsonObject.put("is_city_match_friend_city_center_3m", it[3].getLong("is_city_match_friend_city_center_3m"))
                                jsonObject.put("inter_peer_num_3m", it[4].getInteger("inter_peer_num_3m"))
                                jsonObject.put("friend_num_6m", it[5].getInteger("friend_num_6m"))
                                jsonObject.put("good_friend_num_6m", it[6].getInteger("good_friend_num_6m"))
                                jsonObject.put("friend_city_center_6m", it[7].getString("friend_city_center_6m"))
                                jsonObject.put("is_city_match_friend_city_center_6m", it[8].getLong("is_city_match_friend_city_center_6m"))
                                jsonObject.put("inter_peer_num_6m", it[9].getInteger("inter_peer_num_6m"))
                                jsonResultDataList.add(jsonObject)
                                log.info("查询数据逻辑结束=======001" + Json.encode(jsonResultDataList))
                                log.info("查询数据逻辑结束=======" + jsonResultDataList.size)
                                log.info("查询数据逻辑结束=======" + Json.encode(jsonResultDataList))

                                carrierResultData.task_id = task_id
                                carrierResultData.mobile = mobile
                                carrierResultData.item = "friend_circle"
                                carrierResultData.result = jsonObject.toString()
                                carrierResultDataList.add(carrierResultData)
                            }*/
//                }
//            }


    }

    private fun assemblyParametersList(
        countLess3Months: Single<Int>,
        countLess3MonthsGoupy: Single<Int>,
        countLess3Attribution: Single<String>,
        attributionMobilePhoneNumber: Single<Long>,
        contactPerson: Single<Int>,
        contactPersonLessSixMonth: Single<Int>,
        contactPersonTen: Single<Int>,
        contactPersonTenHomeArea: Single<String>,
        attributionMobilePhoneNumberHun: Single<Long>,
        contactPersonHun: Single<Int>
    ): List<Observable<JsonObject>> {
        var list = listOf(
            countLess3Months.map {
                JsonObject().put("friend_num_3m", it)
            }.toObservable(),
            countLess3MonthsGoupy.map {
                JsonObject().put("good_friend_num_3m", it)
            }.toObservable(),
            countLess3Attribution.map {
                JsonObject().put("friend_city_center_3m", it)
            }.toObservable(),
            attributionMobilePhoneNumber.map {
                JsonObject().put("is_city_match_friend_city_center_3m", it)
            }.toObservable(),
            contactPerson.map {
                JsonObject().put("inter_peer_num_3m", it)
            }.toObservable(),
            contactPersonLessSixMonth.map {
                JsonObject().put("friend_num_6m", it)
            }.toObservable(),
            contactPersonTen.map {
                JsonObject().put("good_friend_num_6m", it)
            }.toObservable(),
            contactPersonTenHomeArea.map {
                JsonObject().put("friend_city_center_6m", it)
            }.toObservable(),
            attributionMobilePhoneNumberHun.map {
                JsonObject().put("is_city_match_friend_city_center_6m", it)
            }.toObservable(),
            contactPersonHun.map {
                JsonObject().put("inter_peer_num_6m", it)
            }.toObservable()
        )
        return list
    }

    /**
     * 统计 近90天月联系人数量（去重）(0-90天)
     */
    fun countLess3Months(mobile: String, taskId: String): Single<Int> {
        log.info("统计 近90天月联系人数量（去重）(0-90天)")
        var sql: String = "SELECT \n" +
                "\t COUNT(DISTINCT peer_number) as \"countTime\"\n" +
                "\t\tFROM\n" +
                "\t\tcarrier_voicecall\n" +
                "\t\twhere \n" +
                "\t\t DATE(date_add(now(), interval -90 day))<\n" +
                "\t\tDATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                "\t\tand  mobile =  '$mobile'\n" +
                "and task_id = '$taskId' "
        return carrierResultDataDao.customizeSQL(sql)
            .map {
                it[0]?.getInteger("countTime")
            }.doOnError {
                log.info("统计 近90天月联系人数量（去重）(0-90天)")
                print(it.printStackTrace())
            }

    }

    /**
     * 近90天联系人数量（联系10次以上，去重）（0-90天）
     */
    fun countLess3MonthsGoupy(mobile: String, taskId: String): Single<Int> {
        log.info("近90天联系人数量（联系10次以上，去重） ")
        var sql: String = "SELECT\n" +
                "\tCOUNT(DISTINCT peer_number) as countTime\n" +
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
                "\t(COUNT(peer_number) > 10)"

        return carrierResultDataDao.customizeSQL(sql)
            .map {
                it[0]?.getInteger("countTime")
            }.doOnError {
                log.info("近90天联系人数量（联系10次以上，去重） ")
                print(it.printStackTrace())
            }
    }

    /***
     * 近90天联系次数最多的号码归属地（0-90天）
     */
    var sql: String = ""

    fun countLess3Attribution(mobile: String, taskId: String): Single<String> {
        log.info("近90天联系次数最多的号码归属地（0-90天）")
        var sql: String = "SELECT homearea from ( \n" +
                "\tSELECT \n" +
                "\tIF(homearea is null or homearea =\"\",\"未匹配\", homearea) as homearea, \n" +
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
                "\tIF(location is null or location =\"\",\"未匹配\", location) as homearea,\n" +
                "\tCOUNT(location) as \"countTime\"\n" +
                "\tfrom carrier_voicecall \n" +
                "\twhere \n" +
                "mobile =  '$mobile'\n" +
                "and task_id =  \"'$taskId'\"\n" +
                "and DATE(date_add(now(), interval - 90 day))<\n" +
                "\tDATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time)) \n" +
                "\tGROUP BY location  \n" +
                " ) as c \n" +
                "\tHAVING(MAX(countTime))"
        return carrierResultDataDao.customizeSQL(sql)
            .map {
                it.firstOrNull().toString()
            }.doOnError {
                log.info("近90天联系次数最多的号码归属地（0-90天）")
                print(it.printStackTrace())
            }
    }

    /***
     * 近90天朋友圈中心城市是否与手机归属地一致（0-90天）
     */
    fun attributionMobilePhoneNumber(mobile: String, taskId: String): Single<Long> {
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
                "\tas result\n" +
                "\tfrom  (\n" +
                "  SELECT MAX(countTime) as countTime,peer_number,homearea from (\n" +
                "  \tSELECT\n" +
                "\tpeer_number as peer_number,\n" +
                "COUNT(peer_number) as \"countTime\",\n" +
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

        return carrierResultDataDao.customizeSQL(sql)
            .map {
                it.first().getLong("result")
            }.doOnError {
                log.info("近90天朋友圈中心城市是否与手机归属地一致（0-90天）")
                print(it.printStackTrace())
            }
    }

    /***
     *  近90天互有主叫和被叫的联系人电话号码数目（去重）（0-90天）
     */
    fun contactPerson(mobile: String, taskId: String): Single<Int> {
        log.info("近90天互有主叫和被叫的联系人电话号码数目（去重）（0-90天）")
        var sql: String = "SELECT COUNT(*)  as \"countTime\"\n" +
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
        var result: Int = 0
        return carrierResultDataDao.customizeSQL(sql)
            .map {
                it[0]?.getInteger("countTime")
            }.doOnError {
                log.info("近90天互有主叫和被叫的联系人电话号码数目（去重）（0-90天）")
                print(it.printStackTrace())
            }
    }


    /***
     *  近180天的联系人数量
     */
    fun contactPersonLessSixMonth(mobile: String, taskId: String): Single<Int> {
        log.info("近180天的联系人数量")
        var sql: String = "SELECT \n" +
                "\t COUNT(DISTINCT peer_number) as \"countTime\"\n" +
                "\t\tFROM\n" +
                "\t\tcarrier_voicecall\n" +
                "\t\twhere \n" +
                "\t\t DATE(date_add(now(), interval -180 day))<\n" +
                "\t\tDATE(CONCAT(SUBSTR(bill_month,1,4),\"-\",time))\n" +
                "\t\tand  mobile =  '$mobile' \n" +
                "and task_id = '$taskId'  "

        return carrierResultDataDao.customizeSQL(sql)
            .map {
                it[0]?.getInteger("countTime")
            }.doOnError {
                log.info("近180天的联系人数量")
                print(it.printStackTrace())
            }
    }

    /***
     *  近180天的联系人数量（联系10次以上，去重）（0-180天）
     */
    fun contactPersonTen(mobile: String, taskId: String): Single<Int> {
        log.info("近180天的联系人数量（联系10次以上，去重）（0-180天）")
        var sql: String = "SELECT\n" +
                "\tCOUNT(DISTINCT peer_number) as countTime\n" +
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
                "\t(COUNT(peer_number) > 10)"
        return carrierResultDataDao.customizeSQL(sql)
            .map {
                it[0]?.getInteger("countTime")
            }.doOnError {
                log.info("近180天的联系人数量（联系10次以上，去重）（0-180天）")
                print(it.printStackTrace())
            }
    }

    /***
     * 近180天的联系次数最多的号码归属地（0-180天）
     */
    fun contactPersonTenHomeArea(mobile: String, taskId: String): Single<String> {
        log.info("近180天的联系次数最多的号码归属地（0-180天）")
        var sql: String = "SELECT homearea from (\n" +
                "\n" +
                "SELECT \n" +
                "IF(homearea is null or homearea =\"\",\"未匹配\", homearea) as homearea,\n" +
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
                "IF(location is null or location =\"\",\"未匹配\", location) as homearea,\n" +
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

        return carrierResultDataDao.customizeSQL(sql)
            .map {
                it[0]?.toString()
            }.doOnError {
                log.info("近180天的联系次数最多的号码归属地（0-180天）")
                print(it.printStackTrace())
            }

    }

    /***
     * 近180天的朋友圈中心城市是否与手机归属地一致（0-180天）
     */
    fun attributionMobilePhoneNumberHun(mobile: String, taskId: String): Single<Long> {
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
                "\tas result\n" +
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

        return carrierResultDataDao.customizeSQL(sql)
            .map {
                it[0]?.getLong("result")
            }.doOnError {
                log.info("近180天的朋友圈中心城市是否与手机归属地一致（0-180天）")
                print(it.printStackTrace())
            }

    }

    /***
     *  近180天的互有主叫和被叫的联系人电话号码数目（去重）（0-180天）
     */
    fun contactPersonHun(mobile: String, taskId: String): Single<Int> {
        log.info("近180天的互有主叫和被叫的联系人电话号码数目（去重）（0-180天）")
        var sql: String = "SELECT COUNT(*)  as \"countTime\"\n" +
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
        return carrierResultDataDao.customizeSQL(sql)
            .map {
                it[0]?.getInteger("countTime")
            }.doOnError {
                log.info("近180天的互有主叫和被叫的联系人电话号码数目（去重）（0-180天）")
                print(it.printStackTrace())
            }
    }
}