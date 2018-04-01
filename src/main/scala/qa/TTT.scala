package qa

object TTT {
  def main(args: Array[String]): Unit = {
    for (i<-1 to 18;j<- 0 to 9){
      var headStr=""
      if(i<10)
        headStr="0"
      var ss="load data inpath 'hdfs://CNSZ22PL0058:8020/zhaoqi/wide_data/wide_data2017-10-"+headStr+i+"-event"+j+"' into table wide_data partition(dt=\"2017-10-"++headStr+i+"\",event_id=\"event"+j+"\");"
      println(ss)
    }
    println("--------------------")
    for (i<-1 to 18;j<- 0 to 9){
      var headStr=""
      if(i<10)
        headStr="0"
      var sw="insert into table test_inc_ubas_app_id0_wide partition(dt=\"2017-10-"+headStr+i+"\",event_id=\"event"+j+"\") select tag,version,remote_host,remote_port,received_msg_id,msg_id,received_time,app_id,app_key,platform,uid,sec_uid,login_type,device_id,cookie_id,ip,time,event_type,page_url,page_name,page_ref,utm_source,utm_medium,utm_term,utm_content,utm_campaign,app_v,sdk,sdk_v,manu,model,os,os_v,s_h,s_w,browser,browser_v,carrier,net_type,longitude,latitude,error_log,error_level,properties,country,province,city,session_id,session_duration,pre_event_id,next_event_id,event_duration,pre_page_url,next_page_url,page_url_duration,pre_diff_page_url,next_diff_page_url,diff_page_url_duration,phone_num,email,gender,birthday,register_time,user_first_visit_time,user_first_visit,device_first_visit_time,device_first_visit,properties_duration,properties_element_content,properties_element_id,properties_element_type,properties_element_msg,properties_from_outside,properties_utm,properties_key1,properties_key2 from wide_data where dt=\"2017-10-"+headStr+i+"\" and event_id=\"event"+j+"\";"
      println(sw)
    }
  }
}

