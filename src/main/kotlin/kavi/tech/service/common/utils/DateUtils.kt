package kavi.tech.service.common.utils

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.temporal.TemporalAdjusters

/**
 * 日期工具类
 */
class DateUtils {

    companion object {

        /**
         * 日期格式
         */
        enum class DatePattern(var value:String){
            YYYY_MM_DD("yyyy-MM-dd"),
            YYYY_MM("yyyyMM")
        }

        /**
         * 时间开始后缀
         */
        private const val BEGIN_SUFFIX = " 00:00:00"
        /**
         * 时间结尾后缀
         */
        private const val END_SUFFIX = " 23:59:59"

        /**
         * 获取当前时间区间
         */
        fun getPreMothInCurrentMoth(num: Long, pattern: String): ArrayList<String> {
            val now = LocalDate.now()
            var moths = arrayListOf<String>()
            for (i in 0..num) {
                moths.add(now.minusMonths(i).format(DateTimeFormatter.ofPattern(pattern)))
            }
            return moths
        }

        /**
         * 获取当前时间、截止时间
         */
        fun getPreMothOrCurrentMoth(num: Long, pattern: String): Pair<String, String> {
            val now = LocalDate.now()
            //之前
            val preYearMoth = now.minusMonths(num).format(DateTimeFormatter.ofPattern(pattern))
            //当前
            val currYearMoth = now.format(DateTimeFormatter.ofPattern(pattern))
            return Pair(preYearMoth, currYearMoth)
        }

        /**
         * 获取月初、月末
         */
        fun getFirstOrLastDate(firstMoth: String, secondMoth: String, pattern: String): Pair<String, String> {
            return Pair(
                LocalDate.parse(
                    firstMoth,
                    DateTimeFormatter.ofPattern(pattern)
                ).with(TemporalAdjusters.firstDayOfMonth()).toString() + BEGIN_SUFFIX,
                LocalDate.parse(
                    secondMoth,
                    DateTimeFormatter.ofPattern(pattern)
                ).with(TemporalAdjusters.lastDayOfMonth()).toString() + END_SUFFIX
            )
        }

        @JvmStatic
        fun main(args: Array<String>) {
            println(this.getFirstOrLastDate("2020-02-01", "2020-04-30", DatePattern.YYYY_MM_DD.value))
        }
    }
}
