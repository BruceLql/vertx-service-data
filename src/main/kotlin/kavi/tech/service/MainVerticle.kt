package kavi.tech.service

import io.vertx.core.json.Json
import kavi.tech.service.service.CallAnalysisService
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

    @Throws(Exception::class)
    override fun start() {
        super.start()
//        vertx.deployVerticle(webVerticle)
        friendSummaryService.toCleaningCircleFriendsData("14779716260", "5e9426345a33e0024df2f20c")?.subscribe({
            println("-------${Json.encode(it)}")
    //            println("-------${it[0].rows[0]}")
    //            println("-------${it[1].rows[0]}")
    //            println("-------${it[2].rows[0]}")
    //            println("-------${it[3].rows[0]}")
    //            println("-------${it[4].rows[0]}")
    //            println("-------${it[5].rows[0]}")
    //            println("-------${it[6].rows[0]}")
    //            println("-------${it[7].rows[0]}")
    //            println("-------${it[8].rows[0]}")
    //            println("-------${it[9].rows[0]}")
    //            println("-------${it[9].rows[0]}")
        },{
            it.printStackTrace()
        })
//        callAnalysisService.toCleaningCircleFriendsData("14779716260", "5e9426345a33e0024df2f20c")

//        callAnalysisService.toCleaningCircleFriendsData("14779716260", "5e9426345a33e0024df2f20c").subscribe({
//
//                        println("-------${it[0].rows}")
//            println("-------${it[1].rows}")
//            println("-------${it[2].rows}")
//            println("-------${it[3].rows}")
//            println("-------${it[4].rows}")
//            println("-------${it[5].rows}")
//            println("-------${it[6].rows}")
//            println("-------${it[7].rows}")
//            println("-------${it[8].rows}")
//            println("-------${it[9].rows}")
//            println("-------${it[10].rows}")
//            println("-------${it[11].rows}")
//            println("-------${it[12].rows}")
//            println("-------${it[13].rows}")
//            println("-------${it[14].rows}")
//            println("-------${it[15].rows}")
//            println("-------${it[16].rows}")
//            println("-------${it[17].rows}")
//            println("-------${it[18].rows}")
//            println("-------${it[19].rows}")
//            println("-------${it[20].rows}")
//            println("-------${it[21].rows}")
//            println("-------${it[22].rows}")
//            println("-------${it[23].rows}")
//            println("-------${it[24].rows}")
//            println("-------${it[25].rows[0]}")
//            println("-------${it[26].rows[0]}")
//            println("-------${it[27].rows[0]}")
//            println("-------${it[28].rows[0]}")
//            println("-------${it[29].rows[0]}")
//            println("-------${it[30].rows[0]}")
////            println("-------" + Gson().toJson(it))
//
//
//        },{
//            it.printStackTrace()
//        })

    }

    companion object {
        @JvmStatic
        fun main(args:Array<String>) {
            // 初始化类
            launcher(MainVerticle::class.java)
        }
    }
}
