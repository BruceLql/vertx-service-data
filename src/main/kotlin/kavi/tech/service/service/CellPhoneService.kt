package kavi.tech.service.service

import io.vertx.core.json.JsonObject
import kavi.tech.service.mysql.dao.CellPhoneDao
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import rx.Observable

/**
 * 获取手机号基本信息
 *
 */
@Service
class CellPhoneService @Autowired constructor(
    private var cellPhoneDao: CellPhoneDao
){
    /**
     * 获取手机号基本信息
     *@param mobile 手机号
     */
    fun getCellPhoneInfo(mobile:String,taskId:String):Observable<List<JsonObject>>{
        var list = ArrayList<JsonObject>()
        return cellPhoneDao.getCellPhoneInfo(mobile,taskId).map {
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
