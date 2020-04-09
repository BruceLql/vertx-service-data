package kavi.tech.service.mongo.model


import io.vertx.rxjava.ext.mongo.MongoClient
import kavi.tech.service.mongo.component.AbstractModel
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import tech.kavi.cms.entity.DataUserInfo


@Repository
class DataOtherInfoModel @Autowired constructor(val client: MongoClient) :
    AbstractModel<DataUserInfo>(client, DataUserInfo.TABLE_NAME, DataUserInfo::class.java) {

    override val log = kavi.tech.service.common.extension.logger(this::class)

    private val tableName = DataUserInfo.TABLE_NAME


}
