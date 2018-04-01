package bounds.pre

import java.io.{File, PrintWriter}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
//过程中部分文件中的经纬度均扩大1000倍,保存为整数形式
// Bounds 第一步 爬取边界值,保存文件 格式为citycode;经度,纬度,经度,纬度
// TBounds 下一步 处理数据边界将精度降低至0.001
// CompletData2 下一步 补全边界值completData2 按照计算机图形学的方式 描点划线
// FinalData 下一步 过滤省份数据,并将边界值按照纬度顺序,一行一行的写入文件,每一行数据格式为 纬度;psdData:经度;下一个psdData:经度...
// SortedData 下一步 将每行数据按照经度从小到大排序,并过滤重复的值,重新写入文件,每行数据格式为 纬度;经度;经度...
// PointDataCode 下一步 取每行数据,将经度右移0.5个单位(即经度+0.0005)获得每个边界点右侧的地区psdCode值 最终的边界数据文件
object Bounds {
  def main(args: Array[String]): Unit = {
    val filepath="D:/phpStudy/WWW/citycode.csv"
    val savepath="D:/phpStudy/WWW/"
    var index=0
    var hnum=1
    val source = Source.fromFile(new File(filepath), "UTF-8")

    val lineIterator = source.getLines
    val jscodes=new ArrayBuffer[String]()
    for (line<-lineIterator){
      if(line.contains(",")){
        val code =line.split(",").apply(1)
        val name =line.split(",").apply(0)
        jscodes+="addBounds(\""+code+"\",\""+name+"\");"
      }
    }
    var writer:PrintWriter=null
    writer = new PrintWriter(new File(savepath+"nopage.html"), "UTF-8")
    writer.write("<!doctype html>\n<html>\n<head>\n    <meta charset=\"utf-8\">\n    <title>绘制行政区划边界</title>\n    <script type=\"text/javascript\" src=\"http://webapi.amap.com/maps?v=1.4.0&key=ed1a0b2eed4eb3801f2938736e53914c\"></script>\n\t<script src=\"jquery-3.2.1.min.js\"></script>\n  </head>\n<body>\n<div id=\"container\"></div>\n<script type=\"text/javascript\">")
    while(index < jscodes.length){
      writer.write(jscodes.apply(index)+"\r\n")
      index+=1
    }
    writer.write(" function addBounds(ccode,cname) {\n        //加载行政区划插件\n        AMap.service('AMap.DistrictSearch', function() {\n            var opts = {\n                subdistrict: 1,   //返回下一级行政区\n                extensions: 'all',  //返回行政区边界坐标组等具体信息\n                level: 'province'  //查询行政级别�? 市\n            };\n            //实例化DistrictSearch\n            district = new AMap.DistrictSearch(opts);\n            district.setLevel('province');\n            //行政区查询\n\n            district.search(ccode, function(status, result) {\n                var bounds = result.districtList[0].boundaries;\n                if (bounds) {\t\n\t\t\t\t\t\n\t\t\t\t\tfor(var i=0,l = bounds.length; i < l; i++){\n\t\t\t\t\t\tsaveToFile(cname,ccode+\"-\"+i,bounds[i]);\t\t\n\t\t\t\t\t}\n                }\n            });\n        });\n    }\n\tfunction saveToFile(cname,savename,bounds){\n\t\t$.ajax({  \n\t\t\ttype : \"POST\",  //提交方式  \n\t\t\turl : \"save.php\",//路径  \n\t\t\tasync : true,\n\t\t\tdata : {  \n\t\t\t\t\"cityname\" : cname,\n\t\t\t\t\"sname\":savename,\n\t\t\t\t\"citybounds\" : bounds.toString()\n\t\t\t}\n\t\t});\t\t\n\t}\n</script>\n</body>\n</html>")
    writer.close()
  }
}

