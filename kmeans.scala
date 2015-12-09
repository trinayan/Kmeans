package fulliris
import scala.io.Source
import scala.util.Random
import scala.collection.mutable.ArrayBuffer
import scala.math._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

/*class Cluster ( points: Array[Array[Double]] ,centroidx :Double,centroidy :Double,centroidw :Double,centroidz :Double, id: Int)*/
class Cluster (points: Array[Array[Double]],centroidx :Double,centroidy :Double,centroidw :Double,centroidz :Double, id: Int)
{
  
  var cluster : Array[Array[Double]]=points;
  
  var clusterid: Int=id;
  var centrex: Double=centroidx;
  var centrey: Double=centroidy;
  var centrew: Double=centroidw;
  var centrez: Double=centroidz;
  
  def addPoint( x: Double, y:Double,w: Double, z:Double, index: Int)
  {
  cluster(index)(0)=x;
  cluster(index)(1)=y;
  cluster(index)(2)=w;
  cluster(index)(3)=z;
  
 }
  
  
  def printcluster()
  {
   for (i<-0 until 100)
   {
     for (j <-0 until 4)
      {
        print( " " +cluster(i)(j))
      }
    println();
    } 
   }
  
  
  def getCentroid()
  {
    print(centrex);
  }
  
  def updateCentroid(count:Int)=
  {
    var sumx=0.0
    var sumy=0.0
    var sumw=0.0
    var sumz=0.0
    
    for ( i<-0 until count)
    {
      
      sumx=sumx+cluster(i)(0);
      sumy=sumy+cluster(i)(1);
      sumw=sumw+cluster(i)(2);
      sumz=sumz+cluster(i)(3);
      
    }
    
    centrex=(1*sumx)/count;
    centrey=(1*sumy)/count;
    centrew=(1*sumw)/count;
    centrez=(1*sumz)/count;
    
  }
  
  
  def distance(x: Double, y: Double, w: Double, z: Double) : Double=
  {
    return sqrt(pow((centrex-x),2)+pow((centrey-y),2)+pow((centrez-z),2)+pow((centrew-w),2));
    
  }

}




object readdata {
  def main(args: Array[String])
  {
    val r=scala.util.Random
   
    val nrows = 150
    val ncols = 4
    val datak = Array.ofDim[Double](nrows, ncols)
    var emptymatrix=Array.ofDim[Double](nrows,ncols)
    var emptymatrix2=Array.ofDim[Double](nrows,ncols)
    var emptymatrix3=Array.ofDim[Double](nrows,ncols)
    val bufferedSource = io.Source.fromFile("Fisher.csv")
    var count = 0
    
    for (line <- bufferedSource.getLines) 
    {
        datak(count) = line.split(",").map(_.trim.toDouble)
        count += 1
        
    }
  
   val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]")
   val sc = new SparkContext(conf)
   val matrix=sc.parallelize(datak);
  
 /* var col1=matrix.map{_(2)}
  
  val sum=col1.reduceLeft[Double](_+_)
  print(sum/150)*/
    
  val cluster1=new Cluster(emptymatrix,2,14,31,52,1);
  val cluster2=new Cluster(emptymatrix2,24,56,30,64,2);
  val cluster3=new Cluster(emptymatrix3,13,56,29,61,3);
  
   val clustermap:Array[Int]=new Array[Int](150);
   var  x,y,w,z :Double=0;
   var dist1:Double=0;
   
   var count1=0;
   var count2=0;
   var count3=0;
  
  
   
   for (k <-0 until 1000)
   {
    
    
     count1=0;
     count2=0;
     count3=0;
     var i=0;
     
     for (a<-0 until nrows)
     {
       x=matrix(a)(0);
       y=matrix(a)(1);
       w=matrix(a)(2);
       z=matrix(a)(3);
       var dist1=cluster1.distance(x,y,w,z);
       var dist2=cluster2.distance(x,y,w,z);
       var dist3=cluster3.distance(x, y, w, z);
        
       
       var distances=List(dist1,dist2,dist3);
       var minimum:Double=distances.min;
       
       
       
        if (minimum == dist1)
       {
         cluster1.addPoint(x,y,w,z,count1)
         count1=count1+1;
         clustermap(i)=0;
         
       }
       else if(minimum == dist2)
       {
         cluster2.addPoint(x,y,w,z,count2)
         count2=count2+1;
         clustermap(i)=1;
       }
        
       else if(minimum == dist3)
       {
         cluster3.addPoint(x,y,w,z,count3)
         count3=count3+1;
         clustermap(i)=2;
       } 
       i=i+1;
        

       
   }
        cluster1.updateCentroid(count1);
        cluster2.updateCentroid(count2);
        cluster3.updateCentroid(count3);
        
     
   }
   
   for (i<-0 until 150)
   {
     print(" "+clustermap(i));
   }
   
}
} 
   

