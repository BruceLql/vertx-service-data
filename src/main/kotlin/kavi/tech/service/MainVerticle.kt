package kavi.tech.service

import kavi.tech.service.mysql.dao.CallLogDao
import kavi.tech.service.service.CallAnalysisService
import kavi.tech.service.service.CarierService
import kavi.tech.service.service.FriendSummaryService
import kavi.tech.service.web.WebVerticle
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Import
import rx.Observable
import tech.kavi.vs.core.LauncherVerticle
import tech.kavi.vs.web.HandlerRequestAnnotationBeanName
import java.util.*

@Import(BeanConfig::class)
@ComponentScan(nameGenerator = HandlerRequestAnnotationBeanName::class)
class MainVerticle : LauncherVerticle() {


    @Autowired
    private lateinit var webVerticle: WebVerticle
    @Autowired
    private lateinit var carierService: CarierService
    @Throws(Exception::class)
    override fun start() {
        super.start()
        vertx.deployVerticle(webVerticle)

        // 查询原始数据封装到result
//        carierService.dataRaw("14779716260","5e944644ccb8428ff6b7dea6").subscribe({
//        carierService.dataRaw("18657763073","5e9e7021b117c5b51d8665df").subscribe({
//            println(it.toString())
//
//
//
//
//        },{it.printStackTrace()})
    }

    companion object {
        @JvmStatic
        fun main(args:Array<String>) {
            // 初始化类
            launcher(MainVerticle::class.java)
        }
    }
}
