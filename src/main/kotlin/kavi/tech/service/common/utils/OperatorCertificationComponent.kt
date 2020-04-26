package kavi.tech.service.common.utils

import io.vertx.core.http.HttpMethod
import io.vertx.core.json.JsonObject
import kavi.tech.service.common.extension.GZIPUtils
import kavi.tech.service.common.extension.logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import rx.Observable
import rx.Single
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

/**
 * 回调组件
 */
@Component
class OperatorCertificationComponent {
    @Autowired
    private lateinit var rxClient: io.vertx.rxjava.ext.web.client.WebClient
    private val log = logger(this::class)

    companion object {
        const val callBackIntervalTime = 5L  //回调间隔时间
        const val maxCallBackTime = 5  //回调次数
        const val callBackTimeOut = 5L  //超时时间
    }

    /**
     * 回调给平台
     * requestParams:请求参数
     * callbackUrl：请求地址
     * callBackName：请求名称
     */
    fun callBackPlatform(resultSend: JsonObject, callbackUrl: String, callBackName: String) {
        var retry = 0
        val pushData = GZIPUtils().compress(resultSend.toString())
        log.info("gzip 压缩后的长度：${pushData?.size}")

        Single.create<String> { subscriber ->

            rxClient.putAbs(callbackUrl).method(HttpMethod.POST)
                .sendStream(Observable.just(io.vertx.rxjava.core.buffer.Buffer.buffer(pushData))) { callBackResutlt ->
                    try{
                        val response = callBackResutlt.result()
                        log.info("[$callBackName],请求回调地址的返回结果:[${callBackResutlt?.result()?.bodyAsString()}]")
                        if (callBackResutlt.failed()) throw callBackResutlt.cause()
                        if (callBackResutlt.result().statusCode() != 200) throw IllegalArgumentException("请求回调地址失败，" +
                                "[${callBackResutlt.result().statusCode()}]:[${callBackResutlt.result().statusMessage()}]")
                        // 获取回调结果
                        val result = callBackResutlt.result().bodyAsJsonObject().getString("code")
                        if(result!="SUCCESS" && result!="success") throw IllegalArgumentException("请求回调未返回success，[$result]")
                        subscriber.onSuccess("")
                    }catch (e:Exception){
                        log.error("[$callBackName],回调平台失败",e)
                        subscriber.onError(e)
                    }
                }

        }.retryWhen { o ->
            o.flatMap { e ->
                retry++
                if (retry < maxCallBackTime) {
                    log.info("[$callBackName],请求回调地址[$callbackUrl] 第[$retry]次失败，将在[${retry * callBackIntervalTime}]s后重新发起请求")
                    Observable.timer(retry * callBackIntervalTime, TimeUnit.SECONDS)
                } else {
                    Observable.error(TimeoutException("[$callBackName],请求回调地址 [$retry]次，已超时，不再请求"))
                }
            }
        }.subscribe({
            log.info("回调[$callBackName]成功")
        }, { e ->
            log.info("回调[$callBackName]成功", e)
        }).also {
            if (it.isUnsubscribed) it.unsubscribe()
        }
    }
}
