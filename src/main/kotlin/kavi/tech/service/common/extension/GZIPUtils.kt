package kavi.tech.service.common.extension

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream

class GZIPUtils {

    val GZIP_ENCODE_UTF_8 = "UTF-8"

    val GZIP_ENCODE_ISO_8859_1 = "ISO-8859-1"

    /**
     * 字符串压缩为GZIP字节数组
     *
     * @param str
     * @return
     */
    public fun compress(str: String?): ByteArray? {
        return compress(str, GZIP_ENCODE_UTF_8)
    }

    /**
     * 字符串压缩为GZIP字节数组
     *
     * @param str
     * @param encoding
     * @return
     */
    public fun compress(str: String?, encoding: String?): ByteArray? {
        if (str == null || str.length == 0) {
            return null
        }
        val out = ByteArrayOutputStream()
        val gzip: GZIPOutputStream
        try {
            gzip = GZIPOutputStream(out)
            gzip.write(str.toByteArray(charset(encoding!!)))
            gzip.close()
        } catch (e: IOException) {
            e.printStackTrace()
        }
        return out.toByteArray()
    }

    /**
     * GZIP解压缩
     *
     * @param bytes
     * @return
     */
   public fun uncompress(bytes: ByteArray?): ByteArray? {
        if (bytes == null || bytes.size == 0) {
            return null
        }
        val out = ByteArrayOutputStream()
        val `in` = ByteArrayInputStream(bytes)
        try {
            val ungzip = GZIPInputStream(`in`)
            val buffer = ByteArray(256)
            var n: Int
            while (ungzip.read(buffer).also { n = it } >= 0) {
                out.write(buffer, 0, n)
            }
        } catch (e: IOException) {
            e.printStackTrace()
        }
        return out.toByteArray()
    }

    /**
     * @param bytes
     * @return
     */
    public fun uncompressToString(bytes: ByteArray?): String? {
        return uncompressToString(
            bytes,
            GZIP_ENCODE_UTF_8
        )
    }

    /**
     * @param bytes
     * @param encoding
     * @return
     */
    public fun uncompressToString(bytes: ByteArray?, encoding: String?): String? {
        if (bytes == null || bytes.size == 0) {
            return null
        }
        val out = ByteArrayOutputStream()
        val `in` = ByteArrayInputStream(bytes)
        try {
            val ungzip = GZIPInputStream(`in`)
            val buffer = ByteArray(256)
            var n: Int
            while (ungzip.read(buffer).also { n = it } >= 0) {
                out.write(buffer, 0, n)
            }
            return out.toString(encoding)
        } catch (e: IOException) {
            e.printStackTrace()
        }
        return null
    }

}
