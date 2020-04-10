package kavi.tech.service.mongo.model


import io.vertx.rxjava.ext.mongo.MongoClient
import kavi.tech.service.mongo.component.AbstractModel
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import kavi.tech.service.mongo.schema.DataCollectionInfo

@Repository
class DataCollectionInfoModel @Autowired constructor(val client: MongoClient) :
    AbstractModel<DataCollectionInfo>(client, DataCollectionInfo.TABLE_NAME, DataCollectionInfo::class.java) {

    override val log = kavi.tech.service.common.extension.logger(this::class)

    private val tableName = DataCollectionInfo.TABLE_NAME


}
