package kavi.tech.service.service

import io.vertx.core.json.JsonObject
import kavi.tech.service.mysql.dao.UserCallDetailsDao
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import rx.Observable


/**
 * 用户通话详单
 */
@Service
class UserCallDetailsService @Autowired constructor(
    private val userCallDetailsDao: UserCallDetailsDao
) {


    /**
     * 用户通话详单
     */
    fun getUserDetails(mobile: String, taskId: String): Observable<List<JsonObject>> {
        return userCallDetailsDao.getUserDetails(mobile, taskId).map { it.rows }
    }
}
