package kavi.tech.service

import io.vertx.rxjava.ext.web.client.WebClient
import kavi.tech.service.service.ContactsRegionService
import kavi.tech.service.service.UserBehaviorService
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
    private lateinit var rxClient: WebClient

    @Autowired
    private lateinit var userBehaviorService: UserBehaviorService

    @Autowired
    private lateinit var  contactsRegionService: ContactsRegionService

    @Throws(Exception::class)
    override fun start() {
        super.start()
        vertx.deployVerticle(webVerticle)
    }

    companion object {
        @JvmStatic
        fun main(args:Array<String>) {
            // 初始化类
            launcher(MainVerticle::class.java)
        }
    }
}
