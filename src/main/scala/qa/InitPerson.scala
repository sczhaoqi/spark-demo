package qa

import java.io.{File, PrintWriter}
import java.util.{HashMap, UUID}

import org.codehaus.jackson.map.ObjectMapper

object InitPerson {
  def main(args: Array[String]): Unit = {
    val writer = new PrintWriter(new File("E:/person.txt"),"UTF-8")
    val objectMapper = new ObjectMapper()
    for(i<-1 to 10) {
      val uid=UUID.randomUUID().toString
      val uname=uid.substring(1,7)
      val passwd=uid.substring(0,18)
      writer.write("\r\n")
      val map2Json = new HashMap[String,Object]
      // 加入list对象
      map2Json.put("id", uid)
      // 加入map对象
      map2Json.put("username", uname)
      // 加入数组对象
      map2Json.put("password", passwd)
      //转化为json字符串
      val json = objectMapper.writeValueAsString(map2Json)
      writer.write(json)
    }
    writer.close()
  }
}