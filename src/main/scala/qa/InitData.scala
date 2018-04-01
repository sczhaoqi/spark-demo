package qa

import java.io.{File, PrintWriter}
import java.util.UUID

import uitls.RandomIp

import scala.util.Random

object InitData {
  def main(args: Array[String]): Unit = {

    val tags=Array("/json_data","/txt_data","/data")
    val versions=Array("V1.0","V1.1","V1.2","V1.3","V2.1")
    val remote_ports=Array("80","8080","3306","9000")
    val login_types=Array("wechat","app","pc")
    val event_ids=Array("event0","event1","event2","event3","event4","event5","event6","event7","event8","event9")
    for(jj<-20 to 30) {
      val dt = "2017-09-" + jj;
      val event_id = event_ids.apply(Random.nextInt(event_ids.length))
      val writer = new PrintWriter(new File("E:/wide_data" + dt +"-"+ event_id + ".txt"), "UTF-8")
      for (i <- 1 to 1000000) {

        val uuid = UUID.randomUUID()
        val ip = RandomIp.getRandomIp
        val received_time = (1504195200 + Random.nextInt(1506787200 - 1504195200))
        val page_name = "page" + Random.nextInt(9)
        val user_first_visit_time = (1475251200 + Random.nextInt(1506787200 - 1475251200))
        writer.write(tags.apply(Random.nextInt(tags.length)))
        //tag	string
        writer.write("," + versions.apply(Random.nextInt(versions.length)))
        //version	string
        writer.write("," + ip)
        // remote_host	string
        writer.write("," + remote_ports.apply(Random.nextInt(remote_ports.length)))
        // remote_port	int
        writer.write("," + UUID.randomUUID())
        //received_msg_id	string
        writer.write("," + UUID.randomUUID())
        //msg_id	string
        writer.write("," + received_time)
        //received_time bigint
        writer.write(",app_id0")
        //app_id	string
        writer.write(",wx4e0fc204d65212a2")
        //app_key	string
        writer.write(",pc")
        //        platform	string
        writer.write("," + uuid)
        //      uid	string
        writer.write(",")
        //      sec_uid                 string
        writer.write("," + login_types.apply(Random.nextInt(login_types.length)))
        //        login_type	string
        writer.write("," + UUID.randomUUID().toString.subSequence(0, 4))
        //        device_id	string 4c8bb3f7-05b9-46c0-943d-652ad3070550
        writer.write(",")
        //        cookie_id	string
        writer.write("," + ip)
        //        ip	string
        writer.write("," + (received_time - Random.nextInt(3000)))
        //        time bigint
        writer.write("," + event_id)
        //        event_type	string
        writer.write(",http://app_id0.sit.sf-express.com/" + page_name)
        //      page_url	string
        writer.write("," + page_name)
        //      page_name	string
        writer.write(",,,,,,,")
        //        page_ref                string
        //        utm_source              string
        //        utm_medium              string
        //        utm_term                string
        //        utm_content             string
        //        utm_campaign            string
        //        app_v                   string
        writer.write(",js")
        //        sdk	string
        writer.write(",1.0.0")
        //        sdk_v	string
        writer.write(",,,,,,")
        //      manu                    string
        //        model                   string
        //        os                      string
        //        os_v                    string
        //        s_h                     int
        //        s_w                     int
        writer.write(",chrome")
        //        browser	string
        writer.write(",53.0.2785.49")
        //        browser_v	string
        writer.write(",,,,,,")
        writer.write(",{\"open_id\":\"wx4e0fc204d65212a2\"}")
        //          properties	map<string,string>
        writer.write(",,,")
        //      country                 string
        //        province                string
        //        city                    string
        writer.write("," + UUID.randomUUID())
        //        session_id	string
        writer.write(",49768000")
        //        session_duration	string
        writer.write(",event" + Random.nextInt(9))
        //        pre_event_id	string
        writer.write(",event" + Random.nextInt(9))
        //        next_event_id	string
        writer.write(",5000")
        //        event_duration	string
        writer.write(",app_id0.sit.sf-express.com/page" + Random.nextInt(9))
        //        pre_page_url	string
        writer.write(",app_id0.sit.sf-express.com/page" + Random.nextInt(9))
        //        next_page_url	string
        writer.write(",5000")
        //        page_url_duration	string
        writer.write(",app_id0.sit.sf-express.com/page" + Random.nextInt(9))
        //        pre_diff_page_url	string
        writer.write(",app_id0.sit.sf-express.com/page" + Random.nextInt(9))
        //        next_diff_page_url	string
        writer.write(",5000")
        //        diff_page_url_duration	string
        writer.write(",,,,,")
        //      phone_num               string
        //        email                   string
        //        gender                  string
        //        birthday                string
        //        register_time           string
        writer.write("," + user_first_visit_time)
        //        user_first_visit_time	bigint
        writer.write("," + Random.nextBoolean())
        //        user_first_visit	boolean
        writer.write("," + user_first_visit_time)
        //        device_first_visit_time	bigint
        writer.write(",flase")
        //      device_first_visit	boolean
        writer.write(",49768000")
        //        properties_duration	string
        writer.write(",,,,,,,,")
        //      properties_element_content      string
        //        properties_element_id   string
        //        properties_element_type string
        //        properties_element_msg  string
        //        properties_from_outside string
        //        properties_utm          string
        //        properties_key1         string
        //        properties_key2         string
        writer.write("\r\n")
      }
      writer.close()
    }
  }
}

