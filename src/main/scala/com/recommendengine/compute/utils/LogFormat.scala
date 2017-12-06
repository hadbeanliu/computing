package com.recommendengine.compute.utils

import java.util.HashMap
import scala.collection.mutable.ArrayBuffer

object LogFormat {

  def main(args: Array[String]): Unit = {

    val String_format = "$remote_addr - $remote_user [$time_local] \"$request\" $status $body_bytes_sent \"$http_referer\" \"$http_user_agent\" \"$http_x_forwarded_for\" \"$cookies_uid";

    val String_log = "127.0.0.1 - - [16/May/2016:17:11:31 +0800] \"GET /cms/emall/webapp/javascript/wSelect.js HTTP/1]+.1\" 200 11660 \"http://www.mricechang.com/cms/goodsPublish.sp?act=publishGoods&caId=2016042816000003&siteId=160131\" \"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.155 Safari/537.36\" \"-\" \"000A\""

    val model = getInstance(String_format)

    val rs = model.parse(String_log)
    println(rs.size())
  }

  def getInstance(format: String): LogFormat = {
    val model = new LogFormat();
    model.init(format)
    model

  }
}

class LogFormat extends Serializable {

  /**
   *
   */
  private val serialVersionUID = 1L
  private var param: Array[String] = null;

  private var piar: Array[String] = null;

  private var first = false;

  private def LogFormat() {
    first = false;
  }

  private def init(format: String): Unit = {
    if (format.charAt(0) == '$')
      first = true;

    val chars = format.toCharArray()
    val pftmp = new ArrayBuffer[String]

    val paramtmp = new ArrayBuffer[String]
    var bf = new StringBuffer();
    var begin = 0;
    var flag = false;
    for (i <- 0 until chars.length) {
      if (chars(i) == '$') {
        if (bf.length() > 0) {
          pftmp.+=(bf.toString());
          bf = new StringBuffer();
        }
        if (flag) {
          paramtmp.+=(format.substring(begin + 1, i));
          flag = false;
        } else {
          begin = i;
          flag = true;
        }

      } else if (String.valueOf(chars(i)).matches("\\w|_")) {
        if (!flag) {
          bf.append(chars(i));

        }
      } else {
        if (flag) {
          paramtmp.+=(format.substring(begin + 1, i));
          flag = false;
        }
        bf.append(chars(i));
      }
    }
    if (flag)
      paramtmp.+=(format.substring(begin + 1));
    if (bf.length() > 0)
      pftmp.+=(bf.toString());

    piar = pftmp.toArray
    param = paramtmp.toArray

  }

  def parse(log: String): java.util.Map[String, String] = {

    val kv = new HashMap[String, String]();
    if (param.length == 0)
      return null;
    if (piar.length == 0) {
      kv.put(param(0), log);
      return kv;
    }

    var i = -1;
    var begin = 0;
    var end = 0;
    var pf1Length = 0;

    if (!first) {
      i = 0;
      pf1Length = piar(0).length;
    }

    val max = log.length();
    var pf1 = "";
    var pf2 = "";
    //		int pfLength = pf1.length();

    for (key <- param) {
      if (piar.length > i + 1)
        pf2 = piar(i + 1);
      if (pf2 eq "")
        end = max;
      else
        end = log.indexOf(pf2, begin + pf1Length);
      kv.put(key, log.substring(begin + pf1Length, end));
      if (end != max) {
        begin = end;

        i += 1;
        pf1 = piar(i);
        pf1Length = pf1.length();
        pf2 = "";
      }
    }
    return kv;
  }
  
 

}