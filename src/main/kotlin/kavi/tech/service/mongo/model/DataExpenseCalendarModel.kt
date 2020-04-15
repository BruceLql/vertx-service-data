package kavi.tech.service.mongo.model


import io.vertx.core.json.JsonObject
import io.vertx.ext.mongo.FindOptions
import io.vertx.ext.sql.UpdateResult
import io.vertx.rxjava.ext.mongo.MongoClient
import kavi.tech.service.mongo.component.AbstractModel
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import kavi.tech.service.mongo.schema.DataExpenseCalendar
import kavi.tech.service.mysql.dao.ExpenseCalendarDao
import rx.Single


@Repository
class DataExpenseCalendarModel @Autowired constructor(val client: MongoClient,val expenseCalendarDao: ExpenseCalendarDao) :
    AbstractModel<DataExpenseCalendar>(client, DataExpenseCalendar.TABLE_NAME, DataExpenseCalendar::class.java) {

    override val log = kavi.tech.service.common.extension.logger(this::class)

    private val tableName = DataExpenseCalendar.TABLE_NAME

    fun queryListAndSave2Mysql(query: JsonObject, findOptions: FindOptions): Single<UpdateResult> {
        val startTime = System.currentTimeMillis()
        return this.client.rxFindWithOptions(tableName, query, findOptions)
            .doAfterTerminate { logger(query, startTime) }.flatMap {
                expenseCalendarDao.expenseCalendarDataInsert(it)
            }
    }
}
