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

package org.xdata.analytics.spark.micropathing.runnables

import java.awt.geom.Point2D
import java.util.Properties

import scala.Array.canBuildFrom
import scala.annotation.serializable

import org.joda.time.DateTime
import org.xdata.analytics.spark.micropathing.MicroPathConfig
import org.xdata.analytics.spark.micropathing.MicroPathEngine

import org.apache.spark.SparkContext


/**
 * Read input from a text file to run micro paths on
 * 
 * see MicroPathEngine.scala
 */
@serializable class TextInputRunnable extends MicroPathEngine{

  
  // read the data, filtering invalid rows and points not in the region.
  // returns (id,dt,x,y)
   override def readInput(sc:SparkContext,config:Properties,triplineConf:MicroPathConfig) = {
    
    println("Running on input file: "+inputPath)
    val lowerLeftAOI = new Point2D.Double(triplineConf.minLongitude,triplineConf.minLatitude)
    val upperRightAOI = new Point2D.Double(triplineConf.maxLongitude,triplineConf.maxLatitude)
    
    val data = sc.textFile(inputPath).map(line =>{
       val tokens = line.trim.split("\t")
      try {
        var year_month_day :Array[Int] = null
        var hour_minute_second :Array[Int] = null
        if (dateTimeCol < 0){
          year_month_day = tokens(dateCol).split('-').map(_.toInt)
          hour_minute_second = tokens(timeCol).substring(0,8).split(':').map(_.toInt)
        }
        else{
          var dateTimeStr = tokens(dateTimeCol)
          dateTimeStr = dateTimeStr.substring(0, dateTimeStr.indexOf('.'))
          val t = if (dateTimeStr.indexOf("T") != -1) "T" else " "
          year_month_day = dateTimeStr.split(t)(0).split('-').map(_.toInt)
          hour_minute_second = dateTimeStr.split(t)(1).substring(0,8).split(':').map(_.toInt)
        }
        val dt = new DateTime(year_month_day(0),year_month_day(1),year_month_day(2),hour_minute_second(0),hour_minute_second(1),hour_minute_second(2))
        val lat = tokens(latCol).toDouble
        val lon = tokens(lonCol).toDouble
        if (betweenpts(lowerLeftAOI,upperRightAOI,new Point2D.Double(lon,lat))){
          (tokens(idCol),dt,lon,lat)
        }
        else{
          null
        }
        
      }catch {
        case e: Exception => {
          e.printStackTrace()
          throw new IllegalArgumentException(e.toString()+" Could not parse date from tokens: "+tokens.reduce(_+","+_)+" column seperator =<"+columnSeperator+">" )
          null
        }
      }
      
    }).filter(_ != null)
    
   data
  }
  
  
  def betweenpts(start:Point2D.Double,end:Point2D.Double,test:Point2D.Double,threshold:Double=0.0000001) :Boolean = {
      val compAxMin = math.min(start.getY,end.getY) - threshold
      val compAxMax = math.max(start.getY,end.getY) + threshold
      val compAyMin = math.min(start.getX,end.getX) - threshold
      val compAyMax = math.max(start.getX,end.getX) + threshold
      compAxMin <= test.getY && test.getY <= compAxMax && compAyMin <= test.getX && test.getX <= compAyMax
  }
  
  

  
}
