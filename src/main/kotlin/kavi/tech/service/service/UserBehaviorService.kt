package kavi.tech.service.service

import io.vertx.core.json.JsonObject
import kavi.tech.service.mysql.dao.UserBehaviorDao
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import rx.Single


/**
 * 用户行为分析
 */
@Service
class UserBehaviorService {

    @Autowired
    private lateinit var userBehaviorDao: UserBehaviorDao

    /**
     * 获取用户行为分析结果（按月汇总近6个月）
     */
    fun getCellBehavior(mobile: String, taskId: String): Single<List<JsonObject>> {
        return userBehaviorDao.getCellBehavior(mobile,taskId).map {
            var json = JsonObject()
            json.put("phone_num", mobile)
            json.put("behavior", it)
            listOf(json)
        }
    }
}
