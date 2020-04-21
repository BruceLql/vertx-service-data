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
     * 查询最近三个月 TOP 10 的朋友圈联系人数据
     */
    fun getnearByThreeMonth(mobile: String, taskId: String): Single<List<JsonObject>> {
        return this.client.rxGetConnection().flatMap { conn ->
            Observable.concat(
                listOf(
                    queryNearByThreeMonth(conn, mobile, taskId).map { it.rows }.toObservable()
                )
            ).toSingle()
        }
    }

    /**
     * 查询最近六个月 TOP 10 的朋友圈联系人数据
     */
    fun getnearBySixMonth(mobile: String, taskId: String): Single<List<JsonObject>> {
        return this.client.rxGetConnection().flatMap { conn ->
            Observable.concat(
                listOf(
                    queryNearBySixMonth(conn, mobile, taskId).map { it.rows }.toObservable()
                )
            ).toSingle()
        }
    }

    /**
     * 查询最近三个月 通话地 TOP 10
     */
    fun queryNearByThreeMonthArea(mobile: String, taskId: String): Single<List<JsonObject>> {
        return this.client.rxGetConnection().flatMap { conn ->
            Observable.concat(
                listOf(
                    queryNearByThreeMonthArea(conn, mobile, taskId).map { it.rows }.toObservable()
                )
            ).toSingle()
        }
    }

    /**
     * 查询最近六个月 通话地 TOP 10
     */
    fun queryNearBySixMonthArea(mobile: String, taskId: String): Single<List<JsonObject>> {
        return this.client.rxGetConnection().flatMap { conn ->
            Observable.concat(
                listOf(
                    queryNearBySixMonthArea(conn, mobile, taskId).map { it.rows }.toObservable()
                )
            ).toSingle()
        }
    }

    /**
     * 查询最近三个月 TOP 10 的朋友圈联系人数据
     */
    fun queryNearByThreeMonth(conn: SQLConnection,
                              mobile: String,
                              taskId: String):Single<ResultSet>{
        var sql:String = "select \n" +
                "cv.location, -- 通话地\n" +
                "cv.peer_number, -- 对方号码\n" +
                "'未知' as '号码类型', -- 号码类型\n" +
                "'未知'as '号码标识', -- 号码标识\n" +
                "count(if((date_sub(curdate(), interval 90 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))),true,null)) as 90_count, -- 通话次数\n" +
                "ifnull(sum(if((date_sub(curdate(), interval 90 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))),cv.duration_in_second,0)),0) as 90_shi_chang, -- 通话时长\n" +
                "count(if((date_sub(curdate(), interval 90 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) && cv.dial_type='DIAL',true,null)) as 90_dial_count,  -- 主叫次数\n" +
                "sum(if((date_sub(curdate(), interval 90 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) && cv.dial_type='DIAL',cv.duration_in_second,0)) as 90_dial_shi_chang,  -- 主叫通话时长\n" +
                "count(if((date_sub(curdate(), interval 90 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) && cv.dial_type='DIALED',true,null)) as 90_dialed_count, -- 被叫次数\n" +
                "sum(if((date_sub(curdate(), interval 90 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) && cv.dial_type='DIALED',cv.duration_in_second,0)) as 90_dialed_shi_chang -- 被叫通话时长\n" +
                "from carrier_voicecall cv where cv.mobile = '$mobile' and cv.task_id='$taskId' group by cv.peer_number order by 90_count desc limit 10;"
        return this.query(conn, sql)
    }

    /**
     * 查询最近六个月 TOP 10 的朋友圈联系人数据
     */
    fun queryNearBySixMonth(conn: SQLConnection,
                              mobile: String,
                              taskId: String):Single<ResultSet>{
        var sql:String = "select \n" +
                "cv.location, -- 通话地\n" +
                "cv.peer_number, -- 对方号码\n" +
                "'未知' as '号码类型', -- 号码类型\n" +
                "'未知'as '号码标识', -- 号码标识\n" +
                "count(if((date_sub(curdate(), interval 180 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))),true,null)) as 180_count, -- 通话次数\n" +
                "ifnull(sum(if((date_sub(curdate(), interval 180 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))),cv.duration_in_second,0)),0) as 180_shi_chang, -- 通话时长\n" +
                "count(if((date_sub(curdate(), interval 180 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) && cv.dial_type='DIAL',true,null)) as 180_dial_count,  -- 主叫次数\n" +
                "sum(if((date_sub(curdate(), interval 180 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) && cv.dial_type='DIAL',cv.duration_in_second,0)) as 180_dial_shi_chang,  -- 主叫通话时长\n" +
                "count(if((date_sub(curdate(), interval 180 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) && cv.dial_type='DIALED',true,null)) as 180_dialed_count, -- 被叫次数\n" +
                "sum(if((date_sub(curdate(), interval 180 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) && cv.dial_type='DIALED',cv.duration_in_second,0)) as 180_dialed_shi_chang -- 被叫通话时长\n" +
                "from carrier_voicecall cv where cv.mobile = '$mobile' and cv.task_id='$taskId' group by cv.peer_number order by 180_count desc limit 10;"
        return this.query(conn, sql)
    }

    /**
     * 查询最近三个月 通话地 TOP 10
     */
    fun queryNearByThreeMonthArea(conn: SQLConnection,
                            mobile: String,
                            taskId: String):Single<ResultSet>{
        var sql:String = "select \n" +
                "cv.location, -- 通话地\n" +
                "IFNULL(COUNT(DISTINCT(peer_number)),0) as countResult, -- 通话号码数\n" +
                "count(if((date_sub(curdate(), interval 90 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))),true,null)) as 90_count, -- 通话次数\n" +
                "ifnull(sum(if((date_sub(curdate(), interval 90 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))),cv.duration_in_second,0)),0) as 90_shi_chang, -- 通话时长\n" +
                "count(if((date_sub(curdate(), interval 90 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) && cv.dial_type='DIAL',true,null)) as 90_dial_count,  -- 主叫次数\n" +
                "sum(if((date_sub(curdate(), interval 90 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) && cv.dial_type='DIAL',cv.duration_in_second,0)) as 90_dial_shi_chang,  -- 主叫通话时长\n" +
                "count(if((date_sub(curdate(), interval 90 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) && cv.dial_type='DIALED',true,null)) as 90_dialed_count, -- 被叫次数\n" +
                "sum(if((date_sub(curdate(), interval 90 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) && cv.dial_type='DIALED',cv.duration_in_second,0)) as 90_dialed_shi_chang-- 被叫通话时长\n" +
                "from carrier_voicecall cv\n" +
                "where cv.mobile = '14779716260' and cv.task_id='5e97f0583ba60bc281e0a3b0' group by cv.location order by 90_count desc LIMIT 10;"
        return this.query(conn, sql)
    }

    /**
     * 查询最近六个月 通话地 TOP 10
     */
    fun queryNearBySixMonthArea(conn: SQLConnection,
                                mobile: String,
                                taskId: String):Single<ResultSet>{
        var sql:String = "select \n" +
                "cv.location, -- 通话地\n" +
                "IFNULL(COUNT(DISTINCT(peer_number)),0) as countResult, -- 通话号码数\n" +
                "count(if((date_sub(curdate(), interval 180 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))),true,null)) as 180_count, -- 通话次数\n" +
                "ifnull(sum(if((date_sub(curdate(), interval 180 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))),cv.duration_in_second,0)),0) as 180_shi_chang, -- 通话时长\n" +
                "count(if((date_sub(curdate(), interval 180 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) && cv.dial_type='DIAL',true,null)) as 180_dial_count,  -- 主叫次数\n" +
                "sum(if((date_sub(curdate(), interval 180 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) && cv.dial_type='DIAL',cv.duration_in_second,0)) as 180_dial_shi_chang,  -- 主叫通话时长\n" +
                "count(if((date_sub(curdate(), interval 180 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) && cv.dial_type='DIALED',true,null)) as 180_dialed_count, -- 被叫次数\n" +
                "sum(if((date_sub(curdate(), interval 180 day) <= date(concat(substr(cv.bill_month,1,4),'-',cv.time))) && cv.dial_type='DIALED',cv.duration_in_second,0)) as 180_dialed_shi_chang-- 被叫通话时长\n" +
                "from carrier_voicecall cv\n" +
                "where cv.mobile = '14779716260' and cv.task_id='5e97f0583ba60bc281e0a3b0' group by cv.location order by 180_count desc LIMIT 10;"
        return this.query(conn, sql)
    }

}