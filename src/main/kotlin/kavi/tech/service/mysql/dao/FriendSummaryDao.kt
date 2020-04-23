package kavi.tech.service.mysql.dao

import io.vertx.core.json.JsonObject
import io.vertx.core.logging.Logger
import io.vertx.ext.sql.ResultSet
import io.vertx.rxjava.ext.asyncsql.AsyncSQLClient
import io.vertx.rxjava.ext.sql.SQLConnection
import kavi.tech.service.mysql.component.AbstractDao
import kavi.tech.service.mysql.entity.ResultData
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import rx.Observable
import rx.Single

/**
 * @packageName kavi.tech.service.mysql.dao
 * @author litiezhu
 * @date 2020/4/20 13:39
 * @Description
 * <a href="goodmanalibaba@foxmail.com"></a>
 * @Versin 1.0
 */
@Repository
class FriendSummaryDao @Autowired constructor(
    private val client: AsyncSQLClient
) : AbstractDao<ResultData>(client) {

    override val log: Logger = kavi.tech.service.common.extension.logger(this::class)


    /**
     * 朋友圈联系人（3/6个月）top统计
     */
    fun getPeerNumTopList(mobile: String,taskId: String): Single<List<JsonObject>> {
        return this.client.rxGetConnection().flatMap { conn ->
            Observable.concat(
                listOf(
                    queryNearByThreeMonth(conn,mobile,taskId).map {
                        JsonObject().put("top_item",it.rows).put("key","peer_num_top3_3m")
                    }.toObservable(),
                    queryNearBySixMonth(conn,mobile,taskId).map {
                        JsonObject().put("top_item",it.rows).put("key","peer_num_top3_6m")
                    }.toObservable()
                )
            ).toList().toSingle().doAfterTerminate(conn::close)
        }
    }

    /**
     * 朋友圈通话地（3/6个月）top统计
     */
    fun getLocationTopList(mobile: String,taskId: String): Single<List<JsonObject>> {
        return this.client.rxGetConnection().flatMap { conn ->
            Observable.concat(
                listOf(
                    queryNearByThreeMonthArea(conn,mobile,taskId).map {
                        JsonObject().put("top_item",it.rows).put("key","location_top3_3m")
                    }.toObservable(),
                    queryNearBySixMonthArea(conn,mobile,taskId).map {
                        JsonObject().put("top_item",it.rows).put("key","location_top3_6m")
                    }.toObservable()
                )
            ).toList().toSingle().doAfterTerminate(conn::close)
        }
    }


    /**
     * 查询最近三个月 TOP 10 的朋友圈联系人数据
     */
    fun queryNearByThreeMonth(conn: SQLConnection,
                              mobile: String,
                              taskId: String):Single<ResultSet>{
        var sql:String = "select \n" +
                "cv.location as peer_num_loc, -- 通话地\n" +
                "cv.peer_number, -- 对方号码\n" +
                "'未知' as group_name, -- 号码类型\n" +
                "'未知'as company_name, -- 号码标识\n" +
                "count(if((date_sub(curdate(), interval 90 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))),true,null)) as call_cnt, -- 通话次数\n" +
                "cast(ifnull(sum(if((date_sub(curdate(), interval 90 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))),cv.duration_in_second,0)),0) AS SIGNED) as call_time -- 通话时长\n" +
                "from carrier_voicecall cv where cv.mobile = '$mobile' and cv.task_id='$taskId' group by cv.peer_number order by call_cnt desc limit 10;"
        return this.query(conn, sql)
    }

    /**
     * 查询最近六个月 TOP 10 的朋友圈联系人数据
     */
    fun queryNearBySixMonth(conn: SQLConnection,
                              mobile: String,
                              taskId: String):Single<ResultSet>{
        var sql:String = "select \n" +
                "cv.location as peer_num_loc, -- 通话地\n" +
                "cv.peer_number, -- 对方号码\n" +
                "'未知' as group_name, -- 号码类型\n" +
                "'未知'as company_name, -- 号码标识\n" +
                "count(if((date_sub(curdate(), interval 180 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))),true,null)) as call_cnt, -- 通话次数\n" +
                "cast(ifnull(sum(if((date_sub(curdate(), interval 180 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))),cv.duration_in_second,0)),0) AS SIGNED) as call_time -- 通话时长\n" +
                "from carrier_voicecall cv where cv.mobile = '$mobile' and cv.task_id='$taskId' group by cv.peer_number order by call_cnt desc limit 10;"
        return this.query(conn, sql)
    }

    /**
     * 查询最近三个月 通话地 TOP 10
     */
    fun queryNearByThreeMonthArea(conn: SQLConnection,
                            mobile: String,
                            taskId: String):Single<ResultSet>{
        var sql:String = "select \n" +
                "cv.location as peer_num_loc, -- 通话地\n" +
                "IFNULL(COUNT(DISTINCT(peer_number)),0) as peer_number_cnt, -- 通话号码数\n" +
                "count(if((date_sub(curdate(), interval 90 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))),true,null)) as call_cnt, -- 通话次数\n" +
                "cast(ifnull(sum(if((date_sub(curdate(), interval 90 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))),cv.duration_in_second,0)),0) AS SIGNED) as call_time, -- 通话时长\n" +
                "count(if((date_sub(curdate(), interval 90 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) && cv.dial_type='DIAL',true,null)) as dial_cnt,  -- 主叫次数\n" +
                "cast(ifnull(sum(if((date_sub(curdate(), interval 90 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) && cv.dial_type='DIAL',cv.duration_in_second,0)),0) AS SIGNED) as dial_time,  -- 主叫通话时长\n" +
                "count(if((date_sub(curdate(), interval 90 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) && cv.dial_type='DIALED',true,null)) as dialed_cnt, -- 被叫次数\n" +
                "cast(ifnull(sum(if((date_sub(curdate(), interval 90 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) && cv.dial_type='DIALED',cv.duration_in_second,0)),0) AS SIGNED) as dialed_time-- 被叫通话时长\n" +
                "from carrier_voicecall cv\n" +
                "where cv.mobile = '$mobile' and cv.task_id='$taskId' group by cv.location order by call_cnt desc LIMIT 10;"
        return this.query(conn, sql)
    }

    /**
     * 查询最近六个月 通话地 TOP 10
     */
    fun queryNearBySixMonthArea(conn: SQLConnection,
                                mobile: String,
                                taskId: String):Single<ResultSet>{
        var sql:String = "select \n" +
                "cv.location as peer_num_loc, -- 通话地\n" +
                "IFNULL(COUNT(DISTINCT(peer_number)),0) as peer_number_cnt, -- 通话号码数\n" +
                "count(if((date_sub(curdate(), interval 180 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))),true,null)) as call_cnt, -- 通话次数\n" +
                "cast(ifnull(sum(if((date_sub(curdate(), interval 180 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))),cv.duration_in_second,0)),0) AS SIGNED) as call_time, -- 通话时长\n" +
                "count(if((date_sub(curdate(), interval 180 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) && cv.dial_type='DIAL',true,null)) as dial_cnt,  -- 主叫次数\n" +
                "cast(ifnull(sum(if((date_sub(curdate(), interval 180 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) && cv.dial_type='DIAL',cv.duration_in_second,0)),0) AS SIGNED) as dial_time,  -- 主叫通话时长\n" +
                "count(if((date_sub(curdate(), interval 180 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) && cv.dial_type='DIALED',true,null)) as dialed_cnt, -- 被叫次数\n" +
                "cast(ifnull(sum(if((date_sub(curdate(), interval 180 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) && cv.dial_type='DIALED',cv.duration_in_second,0)),0) AS SIGNED) as dialed_time-- 被叫通话时长\n" +
                "from carrier_voicecall cv\n" +
                "where cv.mobile = '$mobile' and cv.task_id='$taskId' group by cv.location order by call_cnt desc LIMIT 10;"
        return this.query(conn, sql)
    }

}
