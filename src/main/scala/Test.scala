import java.io.File

import com.example.Employee
import org.apache.avro.file.DataFileWriter
import org.apache.avro.specific.{SpecificDatumWriter, SpecificRecord}

object Test {
  def main(args: Array[String]): Unit = {

    val lines = scala.io.Source.fromResource("employee.csv").getLines()

    val test = new GenericWriter[Employee]
    val dataFileWriter = test.getWriter()
    dataFileWriter.create(Employee.SCHEMA$, new File("~/test2.avro"))
    lines.foreach(line => {
      val cols = line.split(",")

      val ab = new Employee()
      test.fill(ab, cols)

      dataFileWriter.append(ab)
    })


    dataFileWriter.close()
  }
}

class GenericWriter[T <: SpecificRecord] {

  def getWriter(): DataFileWriter[T] = {
    new DataFileWriter[T](new SpecificDatumWriter[T])
  }

  def fill(obj: T, cols: Array[String]) = {
    obj.getClass.getFields.filter(_.getName != "SCHEMA$").zipWithIndex.foreach(field => {
      val s = field._1

      val value = s.getType match {
        case q if q == classOf[Int] => cols(field._2).toInt
        case q if q == classOf[Float] => cols(field._2).toFloat
        case q if q == classOf[Double] => cols(field._2).toDouble
        case q if q == classOf[String] => cols(field._2)
        case q if q == classOf[CharSequence] => cols(field._2)
      }
      //println(value)
      s.set(obj, value)
    })
  }

}