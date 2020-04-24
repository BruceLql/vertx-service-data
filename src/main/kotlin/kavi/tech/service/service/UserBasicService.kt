package kavi.tech.service.service

import io.vertx.core.json.JsonObject
import kavi.tech.service.mysql.dao.UserBasicDao
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import rx.Observable

/**
 * 用户基本信息
 */
@Service
class UserBasicService @Autowired constructor(
    private val userBasicDao: UserBasicDao
) {


    /**
     * 用户基本信息
     */
    fun getUserBasicInfo(idCard: String): Observable<List<JsonObject>> {
        var list = ArrayList<JsonObject>()
        return userBasicDao.getUserBasicInfo(idCard).map {
            if(it.rows.isNotEmpty()){
                it.rows.forEach {
                    it.map{
                        list.add(
                            JsonObject().put("key",it.key).put("value",it.value)
                        )
                    }
                }
            }
            list
        }
    }

}
