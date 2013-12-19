package org.xdata.analytics.spark.micropathing

import java.io.FileInputStream
import java.io.FileInputStream
import java.io.IOException
import java.util.Properties



 /**
   * Provides a simple example run of the MicroPathing analytic. 
   * 
   * Input: see test_data/single_mmsi_ais.txt
   * output: a csv file with lat,lon,weight (weight = number of points aggregated at that spot)
   *  
   * 
   * usage: sbt clean package "run test_data/single_mmsi_ais.tsv output"
   */
object Main{
  
  /**
   * Load the specified configuration file.
   */
  def getConfig(config_file_path:String) : Properties = {
    val config = new Properties()
    try{
       config.load(new FileInputStream(config_file_path))
    } catch {
       case e: IOException => { 
         println("Error reading config file: "+config_file_path); 
         System.exit(1)
       }  
    }
    return config
  }
  

 
  def main(args:Array[String]) = {
    if (args.length != 1 || args(0) == "-h" || args(0) == "--help"){
      println("You must sepcify a config file on the command line")
      System.exit(1);
    }
    run(args(0))
  }
    
  
  def run(config_file_path:String) = {
    val config = getConfig(config_file_path)
    val classname = config.getProperty("micropath.engine")
    val runnable = Class.forName(classname).newInstance()
    runnable match {
      case tripline : MicroPathEngine => tripline.run(config)
      case _ => throw new IllegalArgumentException(classname+" is not an instance of MicroPathEngine. You must specify a MicroPathEngine in your config file.")
    }
  }
  
  
}
  