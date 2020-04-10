package kavi.tech.service.mongo.model


import io.vertx.rxjava.ext.mongo.MongoClient
import kavi.tech.service.mongo.component.AbstractModel
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import kavi.tech.service.mongo.schema.DataAppreciationInfo


@Repository
class DataAppreciationInfoModel @Autowired constructor(val client: MongoClient) :
    AbstractModel<DataAppreciationInfo>(client, DataAppreciationInfo.TABLE_NAME, DataAppreciationInfo::class.java) {

    override val log = kavi.tech.service.common.extension.logger(this::class)

    private val tableName = DataAppreciationInfo.TABLE_NAME


}
