package kavi.tech.service

import io.vertx.core.json.Json
import kavi.tech.service.mysql.dao.FriendSummaryDao
import kavi.tech.service.service.CallAnalysisService
import kavi.tech.service.service.ContactsRegionService
import kavi.tech.service.service.FriendSummaryService
import kavi.tech.service.web.WebVerticle
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Import
import tech.kavi.vs.core.LauncherVerticle
import tech.kavi.vs.web.HandlerRequestAnnotationBeanName

@Import(BeanConfig::class)
@ComponentScan(nameGenerator = HandlerRequestAnnotationBeanName::class)
class MainVerticle : LauncherVerticle() {


    @Autowired
    private lateinit var webVerticle: WebVerticle
    @Autowired
    private lateinit var friendSummaryService: FriendSummaryService
    @Autowired
    private lateinit var callAnalysisService: CallAnalysisService
    @Autowired
    private lateinit var contactsRegionService: ContactsRegionService
    @Autowired
    private lateinit var friendSummaryDao: FriendSummaryDao

    @Throws(Exception::class)
    override fun start() {
        super.start()
        vertx.deployVerticle(webVerticle)
//        friendSummaryService.toCleaningCircleFriendsData("14779716260", "5e9426345a33e0024df2f20c").subscribe({
//            println("-------${Json.encode(it)}")
//        },{
//            it.printStackTrace()
//        })

//        callAnalysisService.toCleaningCircleFriendsData("14779716260", "5e9426345a33e0024df2f20c").subscribe({
//            println("-------${Json.encode(it)}")
//        },{
//            it.printStackTrace()
//        })//
//
//        contactsRegionService.getContactRegion("14779716260", "5e9426345a33e0024df2f20c").subscribe({
//            println("-------${Json.encode(it)}")
//        },{
//            it.printStackTrace()
//        })

        friendSummaryDao.getnearBySixMonth("14779716260", "5e97f0583ba60bc281e0a3b0").subscribe({
            println("-------${Json.encode(it)}")
        },{
            it.printStackTrace()
        })

    }

    companion object {
        @JvmStatic
        fun main(args:Array<String>) {
            // 初始化类
            launcher(MainVerticle::class.java)
        }
    }
}
