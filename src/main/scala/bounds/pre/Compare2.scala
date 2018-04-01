package bounds.pre
import java.io.File
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
object Compare2 {
  def subdirs(dir: File): Iterator[File] = {
    val d = dir.listFiles.filter(_.isDirectory)
    val f = dir.listFiles.toIterator
    f ++ d.toIterator.flatMap(subdirs _)
  }
  def main(args: Array[String]): Unit = {
    val filePath1="D:\\phpStudy\\WWW\\save\\"
    val citypath="D:\\phpStudy\\WWW\\citycode.csv"
    val s1=subdirs(new File(filePath1))
    val files1=new ArrayBuffer[String]()
    s1.foreach(x=>{
      val code=x.getName.split("-").apply(0)
      if(!files1.contains(code))
        files1+=code
    })
    val source= Source.fromFile(new File(citypath), "UTF-8")
    val iterator=source.getLines()
    for (line <- iterator){
      if(line.contains(",")){
        val code=line.split(",").apply(1)
        if(!files1.contains(code)&& !line.contains("市辖�?")){
          println(line)
        }
      }
    }
  }
}

