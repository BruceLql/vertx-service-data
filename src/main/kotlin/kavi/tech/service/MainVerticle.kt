package kavi.tech.service

import io.vertx.ext.mongo.FindOptions
import kavi.tech.service.service.ReportService
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
    private lateinit var reportService: ReportService

    @Throws(Exception::class)
    override fun start() {
        super.start()
//        vertx.deployVerticle(webVerticle)
        val name  = "张三"
        val idCard = "360731199900002222"
        val mobile = "18016875613"
        val taskId = "5ebba3a74c4dd0396012b455"
//        val taskId = "5ebcf52972f96dce85c178ef"
        val query = io.vertx.core.json.JsonObject()
        query.put("mobile", mobile).put("mid", taskId)


        //   数据提取 根据传进来的task_id开始从mongo中读取数据 以及简单清洗后存入Mysql
        reportService.beginDataByMongo(query, FindOptions(), name, idCard).subscribe({
            println("---------------------:${it.toString()}")
        }, { it.printStackTrace() })

    }

    companion object {
        @JvmStatic
        fun main(args:Array<String>) {
            // 初始化类
            launcher(MainVerticle::class.java)
        }
    }
}
