import scala.io.Source
import scala.util.Random
import scala.collection.mutable.ArrayBuffer
import scala.math._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level


object kmeans{

 def main(args: Array[String])
  {
    
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]")

    val nrows = 150
    val ncols = 4
    val matrix = Array.ofDim[Double](nrows, ncols)
	
    val bufferedSource = io.Source.fromFile("Fisher.csv")

    var count = 0
    
    for (line <- bufferedSource.getLines) 
    {
        matrix(count) = line.split(",").map(_.trim.toDouble)
        count += 1
    }

    val newmatrix=sc.parallelize(matrix);


}
}