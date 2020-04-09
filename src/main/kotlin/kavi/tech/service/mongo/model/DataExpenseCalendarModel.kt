package kavi.tech.service.mongo.model


import io.vertx.rxjava.ext.mongo.MongoClient
import kavi.tech.service.mongo.component.AbstractModel
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import tech.kavi.cms.entity.DataExpenseCalendar


@Repository
class DataExpenseCalendarModel @Autowired constructor(val client: MongoClient) :
    AbstractModel<DataExpenseCalendar>(client, DataExpenseCalendar.TABLE_NAME, DataExpenseCalendar::class.java) {

    override val log = kavi.tech.service.common.extension.logger(this::class)

    private val tableName = DataExpenseCalendar.TABLE_NAME


}