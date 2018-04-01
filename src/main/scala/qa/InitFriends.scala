package qa

import java.io.{File, PrintWriter}

import scala.util.Random

object InitFriends {
  def main(args: Array[String]): Unit = {
    val writer = new PrintWriter(new File("E:/friends.txt"))
    for(i<-1 to 10000) {
      val useid=Random.nextInt(100)
      val friendid=Random.nextInt(100)
      if(useid!=friendid)
        writer.write("01367"+useid+","+"01367"+friendid+"\n")
    }
    writer.close()
  }
}

