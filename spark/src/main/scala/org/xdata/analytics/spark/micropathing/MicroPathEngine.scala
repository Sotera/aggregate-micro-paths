/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *       http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.xdata.analytics.spark.micropathing


import java.lang.{Double => JavaDouble}

import java.awt.geom.Point2D
import java.util.Properties
import java.util.ArrayList;

import scala.annotation.serializable
import scala.collection.mutable.ListBuffer

import org.joda.time.DateTime

import com.oculusinfo.binning.TileData
import com.oculusinfo.binning.TileIndex
import com.oculusinfo.binning.impl.WebMercatorTilePyramid

import com.oculusinfo.tilegen.tiling.StandardDoubleBinDescriptor
import com.oculusinfo.tilegen.tiling.StandardDoubleArrayBinDescriptor
import com.oculusinfo.tilegen.tiling.HBaseTileIO
import com.oculusinfo.tilegen.tiling.LocalTileIO

import org.apache.spark.SparkContext

import org.apache.spark.rdd.RDD

/**
 *  Executes all stages of micro pathing analytic.
 * 
 * Extend this trait and override the readInput method (or use an available class)
 * 
 */
trait MicroPathEngine {
  
  var inputPath = ""
  var outputPath = ""
  var idCol = -1
  var latCol = -1
  var lonCol = -1
  var dateTimeCol = -1
  var dateCol = -1
  var timeCol = -1
  var columnSeperator = ""
  
  
  def run(config:Properties) = {
    checkConf(config)
    readStandardConfProperties(config)
    val sc = setup(config)
    val triplineConf = getMicroPathConf(config)
    val data = readInput(sc,config,triplineConf)
    val paths = processPaths(sc,config,triplineConf,data)
    execute(sc,paths,config,triplineConf)  
    
  }
  
  /**
   * enforce required conf file properties
   */
  def checkConf(conf:Properties){
   
  }
  
  def readStandardConfProperties(conf:Properties) = {
    inputPath = conf.getProperty("input.path")
    outputPath = conf.getProperty("output.path")
    idCol = conf.getProperty("col.id","0").toInt
    latCol = conf.getProperty("col.lat","1").toInt
    lonCol = conf.getProperty("col.lon","2").toInt
    dateTimeCol = conf.getProperty("col.datetime","-1").toInt
    dateCol = conf.getProperty("col.date","-1").toInt
    timeCol = conf.getProperty("col.time","-1").toInt
    columnSeperator = conf.getProperty("col.seperator",0x1.toChar.toString) // default hive field terminator 
  }
  
  /**
   * Create the trip line analytic configuration from the given properties file
   */
  def getMicroPathConf(conf:Properties) = {
    val timeFilter = conf.getProperty("time.filter",Int.MaxValue.toString).toInt
    val distanceFilter = conf.getProperty("distance.filter",Int.MaxValue.toString).toInt
    val lowerLat = conf.getProperty("lower.lat","-90.0").toDouble
    val lowerLon = conf.getProperty("lower.lon","-180.0").toDouble
    val upperLat = conf.getProperty("upper.lat","90.0").toDouble
    val upperLon = conf.getProperty("upper.lon","179.999999").toDouble
    val regionWidth = conf.getProperty("tripline.region.width","100000.0").toDouble
    val regionHeight  = conf.getProperty("tripline.region.height","100000.0").toDouble
    val velocityFilter = conf.getProperty("velocity.filter","-1.0").toDouble
    new MicroPathConfig(timeFilter, distanceFilter, velocityFilter, lowerLat, lowerLon, upperLat, upperLon, regionWidth, regionHeight)
  }
  
  
  
  /**
   * Create the spark context from the properties file.
   */
  def setup(config:Properties): SparkContext = {
    val default_parallelism = config.getProperty("default_parallelism","8").toInt
    val frameSize = config.getProperty("spark.akka.frameSize","200")
    val master_uri = config.getProperty("master_uri","local")
    val spark_home = config.getProperty("SPARK_HOME","")
    val deploymentCodePaths = config.getProperty("deployment_path","").split(":")
    val jobName = config.getProperty("job.name","SparkMicroPathing")
    

    // System.setProperty("spark.executor.memory", "6G")

    System.setProperty("spark.default.parallelism",default_parallelism.toString)
    println("spark.default.parallelism "+default_parallelism.toString)
    System.setProperty("spark.akka.frameSize",frameSize)
    println("spark.akka.frameSize "+frameSize)
    System.setProperty("spark.storage.memoryFraction", "0.5")
    println("spark.storage.memoryFraction 0.5")

    System.setProperty("spark.worker.timeout", "30000")
    System.setProperty("spark.akka.timeout", "30000")
    System.setProperty("spark.storage.blockManagerHeartBeatMs", "30000")

    val checkPointDir = config.getProperty("checkpoint.dir","/tmp/checkpoints")
    println("check point dir: "+checkPointDir)
   
    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //System.setProperty("spark.kryo.registrator", "org.xdata.analytics.spark.micropathing.TriplineDataRegistrator")
    
    val sc:SparkContext = 
      if (master_uri.indexOf("local") == 0)
        new SparkContext( master_uri, jobName)
          else
            new SparkContext( master_uri, jobName, spark_home, deploymentCodePaths)
    
    sc.setCheckpointDir(checkPointDir, true)
    return sc
    
  }
  
  
  
  /**
   * Read input data into an RDD[MicroPathInputRow]
   * 
   * OVERRIDE this method to read you input data. Ensure you filter out data that is not inside the test region.
   * 
   */
  def readInput(sc:SparkContext,config:Properties,triplineConf:MicroPathConfig) :RDD[(String,DateTime,Double,Double)]
  
  
  /**
   * Convert the raw input data into paths, and cache for future use.
   */
  def processPaths(sc:SparkContext,config:Properties,triplineConf:MicroPathConfig,data:RDD[(String,DateTime,Double,Double)]) : RDD[(Point2D.Double,Point2D.Double,Double)] = {
    
    val pathRDD = data.groupBy(_._1)
    .flatMap( {case (id,dataSeq) =>
      val buffer = new ListBuffer[(Point2D.Double,Point2D.Double,Double)]
      val sorted = dataSeq.sortWith({case(row1,row2) => row1._2.compareTo(row2._2) < 0 })
      var prevPoint : Point2D.Double = null
      var prevTime :DateTime = null
      
      for (row <- sorted){
        val currentPoint = new Point2D.Double(row._3,row._4)
        val currentTime = row._2
        if (prevPoint != null){
          val distance = MicroPathUtilities.getDistance(prevPoint,currentPoint)
          val timeDelta = MicroPathUtilities.getTimeDelta(currentTime,prevTime)
          val velocity = MicroPathUtilities.getVelocity(distance,timeDelta)
          if ( distance > 0 && distance < triplineConf.distanceFilter && 
              timeDelta < triplineConf.timeFilter && 
              (velocity < triplineConf.velocityFilter || triplineConf.velocityFilter < 0) 
        	){
             buffer += ((prevPoint,currentPoint,velocity))
          }
        }
        prevPoint = currentPoint
        prevTime = currentTime
        
      }
      buffer
    }).cache  // cache the paths as they will be used multiple times (once for each level)
    //pathRDD.checkpoint
    return pathRDD
  }
  
  
  /**
   * Execute the analytic and return weighted coordinates.
   */
  def execute(sc:SparkContext,paths:RDD[(Point2D.Double,Point2D.Double,Double)],config:Properties,triplineConf:MicroPathConfig) = {
    val maxLevel = config.getProperty("mercator.level","1").toInt
    
    var name = config.getProperty("avro.output.name","unkown")
    var desc = config.getProperty("avro.output.desc","unknown")
    val datastore = config.getProperty("avro.data.store","local")
    
    println("paths: " + paths.count())

    for (level <- 1 to maxLevel){
      val t1 = System.currentTimeMillis()
      
      /*
      val decay = config.getProperty("velocity.filter.decay","-1.0").toDouble
      val filteredPaths = if (decay >0 && decay < 1 && level > 0) paths.filter({case (p1,p2,v) => 
       v < (math.pow(decay,level-1)*triplineConf.velocityFilter) || triplineConf.velocityFilter < 0
      }) else paths
      */
      
      val tileRDD = paths.flatMap( {case(p1,p2,v)=> MicroPathUtilities.findPath(p1,p2,level)})
      .groupBy({case ((tileX,tileY,binX,binY)) => (tileX,tileY) })
      .map({case (tilePoint,dataSeq) => 
         val freqs = new Array[Double](256*256)
         for ((tileX,tileY,binX,binY) <- dataSeq){
           val index = binX+(binY*256)
           freqs(index) += 1
         } 
         var tileData = new ArrayList[JavaDouble](256*256)
         for (freq <- freqs) {
            tileData.add(freq)
         }
         
         // TODO - do an additional aggregation step on bins here. check to see if visual is better
         
         // END
         val tileIndex = new TileIndex(level,tilePoint._1,tilePoint._2)
         new TileData[JavaDouble](tileIndex, tileData)
      })
      // tileRDD.cache // cache before writing to ensure computation of RDD is not repeated.

      val pyramider = new WebMercatorTilePyramid()

      var io = if (datastore =="hbase") new HBaseTileIO(config.getProperty("hbase.zookeeper.quorum"),
                                                        config.getProperty("hbase.zookeeper.port"),
                                                        config.getProperty("hbase.master")) 
               else new LocalTileIO("avro")
      
      var binDesc = new StandardDoubleBinDescriptor()
      
      // if using hdfs output path should be the path in hdfs, if hbase outputPath is the table name
      io.writeTileSet[Double, JavaDouble](pyramider, 
                                          outputPath, 
                                          tileRDD, 
                                          binDesc, 
                                          name, 
                                          desc)
      //io.writeTiles(outputPath, pyramider, serializer, tileRDD) // config.getProperty("avro.output.name","unkown"), config.getProperty("avro.output.desc","unknown"))
      //tileRDD.unpersist
      val time = System.currentTimeMillis() - t1
      println("Computed level "+level+" in "+time+" msec.")
    }
    
  }
  
 
  
  
  
  


}
