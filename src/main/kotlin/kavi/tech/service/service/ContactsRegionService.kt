package kavi.tech.service.service

import io.vertx.core.json.JsonObject
import kavi.tech.service.mysql.dao.ContactsRegionDao
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import rx.Single


/**
 * 联系人区域汇总
 */
@Service
class ContactsRegionService {

    @Autowired
    private lateinit var contactsRegionDao: ContactsRegionDao

    /**
     * 获取用户3个月、6个月按区域分析联系人
     */
    fun getContactRegion(mobile:String,taskId:String):Single<List<JsonObject>>{
        return contactsRegionDao.getContactRegion(mobile,taskId)
    }

}
