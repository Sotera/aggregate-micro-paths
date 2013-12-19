package org.xdata.analytics.spark.micropathing

import java.awt.geom.Point2D
import scala.collection.mutable.ListBuffer
import org.joda.time.DateTime
import com.oculusinfo.binning.impl.WebMercatorTilePyramid
import com.oculusinfo.binning.BinIndex
import com.oculusinfo.binning.TileIndex

object MicroPathUtilities {

  // in seconds
  def getTimeDelta(dt1:DateTime,dt2:DateTime) = {
    math.abs(dt1.getMillis() - dt2.getMillis())/1000
  }
   
  // distance between two points on the globe
  def getDistance(p1:Point2D.Double,p2:Point2D.Double) = {
    def wrapDistance(d1:Double,d2:Double) = {
      if (d1 < -90 && d2 > 90) (d1,d2-360)
      else if (d2 < -90 && d1 > 90) (d1-360,d2)
      else (d1,d2)
    }
    
    val (alat,blat) = wrapDistance(p1.y,p2.y)
    val (alon,blon) = wrapDistance(p1.x,p2.x)
    val R = 6371000.0
    val dlat = math.toRadians(alat-blat)
    val dlon = math.toRadians(alon-blon)
    
    val a = math.sin(dlat/2) * math.sin(dlat/2) + math.cos(math.toRadians(alat)) * math.cos(math.toRadians(blat)) * math.sin(dlon/2) * math.sin(dlon/2)
    val c = 2 * math.atan2(math.sqrt(a),math.sqrt(1-a))
    R * c  // return the distance as a function of the lat/lon pair
  }
  
  
  // km/hour
  def getVelocity(distance:Double,timeDelta:Long) = {
    (distance/1000) / (timeDelta/3600.0)
  }
  
  /**
   * For two points and a level produce a set of tile/bin coordiantes
   * for the path between the two points.
   * 
   * output:  (tileX,tileY,binX,binY)
   */
  def findPath(start:Point2D,finish:Point2D,tileLevel:Int) = {
    val pyramid = new WebMercatorTilePyramid();
    val tile1 = pyramid.rootToTile(start, tileLevel)
    val bin1 = pyramid.rootToBin(start, tile1)
    val tile2 = pyramid.rootToTile(finish, tileLevel)
    val bin2 = pyramid.rootToBin(finish, tile2)
    val maxbins = (math.pow(2,tileLevel) * 256).toLong
    
    val buffer = new ListBuffer[(Int,Int,Int,Int)]()
    
    def binToPlaneCoordinates(tile:TileIndex,bin:BinIndex) = {
      val x = tile.getX() * 256L + bin.getX()
      val y = tile.getY() * 256L + (256 - bin.getY()) // y-axis is inverse
      (x,y)
    }
    
    def planeToBinCoordinates(x:Long,y:Long) = {
      val _x = if (x < 0 ) x + maxbins else x
      val _y = if (y < 0) y + maxbins else y
      val tileX = (_x /256).toInt
      val tileY = (_y / 256).toInt
      val binX = (_x % 256).toInt
      val binY = (255 - (_y % 256)).toInt
      ((tileX,tileY,binX,binY))
    }
   
   
    var p1 = binToPlaneCoordinates(tile1,bin1)
    var p2 = binToPlaneCoordinates(tile2,bin2)
    
    // wrap points around globe if needed.
    if (math.abs(p1._1 - p2._1) > (maxbins*.5) ){
      if (p1._1 > p2._1) p1 = ((p1._1 - maxbins,p1._2))
      else p2 = ((p2._1 - maxbins,p2._2))
    } 
    if (math.abs(p1._2 - p2._2) > (maxbins*.5) ){
      if (p1._2 > p2._2) p1 = ((p1._1,p1._2 - maxbins))
      else p2 = ((p2._1,p2._2-maxbins))
    } 
    
    
    val rise = (p2._2 - p1._2).toFloat
    val run = (p2._1 - p1._1).toFloat
    
   
    if (0 == run){  // vertical line
      for (y <- math.min(p1._2,p2._2) to math.max(p1._2,p2._2)){
        buffer += planeToBinCoordinates(p1._1,y)
      }
    }
    else {  // linear equation
      val b = p1._2 - (p1._1*rise/run)
      def y(x:Long)= (x*rise/run +b).toInt
      for (x <- math.min(p1._1,p2._1) to math.max(p1._1,p2._1)){
        buffer += planeToBinCoordinates(x,y(x))
      }
    }  
    
    
    buffer
    
  }
  
  
}