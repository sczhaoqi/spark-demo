package hadoop

import java.io.{BufferedWriter, FileOutputStream, OutputStreamWriter}

class PartitionWriterHolder() {
  var arrayWriter=new Array[BufferedWriter](100)
  def write(partitionNum:Int,any: Any): Unit ={
    if(arrayWriter(partitionNum)==null){
      val writer =new BufferedWriter(new OutputStreamWriter(new FileOutputStream("E:/num/numbers"+partitionNum+".txt", false)))
      arrayWriter(partitionNum)=writer
    }
    arrayWriter(partitionNum).write(any.toString+"\r\n")
  }
  def close(): Unit ={
    for(i<-0 to arrayWriter.length-1) {
      if(arrayWriter(i)!=null)
        arrayWriter(i).close
      arrayWriter(i)=null
    }
  }
}

