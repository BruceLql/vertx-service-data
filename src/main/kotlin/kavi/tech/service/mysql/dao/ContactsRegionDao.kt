package kavi.tech.service.mysql.dao

import io.vertx.core.json.JsonObject
import io.vertx.core.logging.Logger
import io.vertx.ext.sql.ResultSet
import io.vertx.rxjava.ext.asyncsql.AsyncSQLClient
import io.vertx.rxjava.ext.sql.SQLConnection
import kavi.tech.service.common.extension.logger
import kavi.tech.service.common.utils.DateUtils
import kavi.tech.service.mysql.component.AbstractDao
import kavi.tech.service.mysql.entity.ResultData
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import rx.Observable
import rx.Single

/**
 * 联系人区域汇总
 */
@Repository
class ContactsRegionDao @Autowired constructor(
    private val client: AsyncSQLClient
) : AbstractDao<ResultData>(client) {

    override val log: Logger = logger(this::class)


    /**
     * 获取联系人区域
     */
    fun getContactRegion(mobile: String, taskId: String): Single<List<JsonObject>> {
        //3月
        val moth3 = DateUtils.getPreMothOrCurrentMoth(3L, DateUtils.Companion.DatePattern.YYYY_MM_DD.value)
        val mothPair3 = DateUtils.getFirstOrLastDate(moth3.first, moth3.second, DateUtils.Companion.DatePattern.YYYY_MM_DD.value)

        //6月
        val moth6 = DateUtils.getPreMothOrCurrentMoth(6L, DateUtils.Companion.DatePattern.YYYY_MM_DD.value)
        val mothPair6 = DateUtils.getFirstOrLastDate(moth6.first, moth6.second, DateUtils.Companion.DatePattern.YYYY_MM_DD.value)
        return this.client.rxGetConnection().flatMap { conn ->
            Observable.concat(
                listOf(
                    getDataByMoth(conn, mobile, taskId, mothPair3).map { it.rows }.toObservable(),
                    getDataByMoth(conn, mobile, taskId, mothPair6).map { it.rows }.toObservable()
                )
            ).toList().toSingle().doAfterTerminate(conn::close)
        }.map {
            (0..1).map { d ->
                var json = JsonObject()
                json.put("key", "contact_region_${if(d==0) 3 else 6}m")
                json.put("desc", "联系人手机号码归属地 (近${if(d==0) 3 else 6}月联系次数降序)")
                json.put("region_list", it[d])
            }
        }
    }

    /**
     * 查询最近三个月、六个月数据
     */
    fun getDataByMoth(
        conn: SQLConnection,
        mobile: String,
        taskId: String,
        mothPair: Pair<String, String>
    ): Single<ResultSet> {
        val sql = "select\n" +
                "    b.region_loc,\n" +
                "    b.region_uniq_num_cnt,\n" +
                "    b.region_call_cnt,\n" +
                "    b.region_call_time,\n" +
                "    b.region_dial_cnt,\n" +
                "    b.region_dial_time,\n" +
                "    b.region_dialed_cnt,\n" +
                "    b.region_dialed_time,\n" +
                "    (b.region_dial_time / b.region_dial_cnt) as region_avg_dial_time,\n" +
                "    (b.region_dialed_time / b.region_dialed_cnt) as region_avg_dialed_time,\n" +
                "    (b.region_dial_cnt / b.region_uniq_num_cnt) as region_dial_cnt_pct,\n" +
                "    (b.region_dial_time / b.region_call_time) as region_dial_time_pct,\n" +
                "    (b.region_dialed_cnt / b.region_uniq_num_cnt) as region_dialed_cnt_pct,\n" +
                "    (b.region_dialed_time / b.region_call_time) as region_dialed_time_pct\n" +
                "from (\n" +
                "         select\n" +
                "             cv.homearea as region_loc,\n" +
                "             count(distinct cv.peer_number) as region_uniq_num_cnt,\n" +
                "             count(*) as region_call_cnt,\n" +
                "             ifnull(sum(cv.duration_in_second),0) as region_call_time,\n" +
                "             count(if(cv.dial_type='DIAL',true,null)) as region_dial_cnt,\n" +
                "             ifnull(sum(if(cv.dial_type='DIAL',cv.duration_in_second,0)),0) as region_dial_time,\n" +
                "             count(if(cv.dial_type='DIALED',true,null)) as region_dialed_cnt,\n" +
                "             ifnull(sum(if(cv.dial_type='DIALED',cv.duration_in_second,0)),0) as region_dialed_time\n" +
                "         from carrier_voicecall cv where cv.mobile='$mobile' and cv.task_id='$taskId' and cv.time between '${mothPair.first}' and '${mothPair.second}' group by cv.homearea) as b order by b.region_call_cnt desc"
        return this.query(conn, sql)
    }
}
