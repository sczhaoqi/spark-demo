package utils

import scala.util.Random

object StringTool {
  /***
    * @param len 生成的字符串的长度
    * @param complex 是否大小写混用
    * @param IsCL 非混用模式下,当前是否只使用大写字符
    * @return
    */
  def RandomString(len:Int,complex:Boolean,IsCL:Boolean=false):String={
    var s=""
    val a='a'
    val A='A'
    for (i<-1 to len) {
      if(!complex&&IsCL) {
        s += (A + Random.nextInt(26)).toChar
      }else if(complex&&Random.nextBoolean()){
        s+=(A+Random.nextInt(26)).toChar
      }else {
        s += (a + Random.nextInt(26)).toChar
      }
    }
    s
  }
  def RanddomString(len:Int): String ={
    RandomString(len,true)
  }
}