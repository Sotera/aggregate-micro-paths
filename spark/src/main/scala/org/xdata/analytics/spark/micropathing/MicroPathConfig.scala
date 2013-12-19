package org.xdata.analytics.spark.micropathing

/**
 * Configuration object to control micropathing analytic behavior
 * sets the trip line blankets and time and distance filters
 */
@serializable class MicroPathConfig(){
  
  
  def this(time_filter:Int,distance_filter:Int,velocity_filter:Double,lower_lat:Double,lower_long:Double,upper_lat:Double,upper_long:Double,width:Double,height:Double) = {
    this()
    timeFilter = time_filter
    distanceFilter = distance_filter
    velocityFilter = velocity_filter
    minLatitude = lower_lat
    maxLatitude = upper_lat
    minLongitude = lower_long
    maxLongitude = upper_long
    segmentWidth = width
    segmentHeight = height
  }
  
  // velocity filter, negitive values are ignored.  km/hr
  var velocityFilter = -1.0
  
  // time filter, in seconds  - don't connect if delta time > time filter
  var timeFilter = Int.MaxValue
  
  // distance filter in meters, don't connect points if delta distance is > distance filter
  var distanceFilter = Int.MaxValue  
  
  // size of trip line boxes in kilometers
  var segmentWidth = 100000.0
  var segmentHeight = 100000.0
  
  /**
   * Minimum Latitude value for the trip line region
   * must be between -90 and 90 inclusive
   */
  private var _minLatitude = -90.0
  def minLatitude = _minLatitude
  def minLatitude_= (value:Double):Unit = {
    verifyLat(value)
    _minLatitude = value
  }
  
  /**
   * Minimum Longitude value for the trip line region
   * must be between -180.0 and 179.999999 inclusive
   */
  private var  _minLongitude = -180.0
  def minLongitude = _minLongitude
  def minLongitude_= (value:Double):Unit = {
    verifyLong(value)
    _minLongitude = value
  }
  
  /**
   * Maximum latitude for the trip line region
   * must be between -90 and 90 inclusive
   */
  var _maxLatitude = 90.0
  def maxLatitude = _maxLatitude
  def maxLatitude_= (value:Double):Unit = {
    verifyLat(value)
    _maxLatitude = value
  }
  
  /**
   * Maximum Longitude for the trip line region
   * must be between -180 and 179.999999 inclusive
   */
  var _maxLongitude = 179.999999
  def maxLongitude = _maxLongitude
  def maxLongitude_= (value:Double):Unit = {
    verifyLong(value)
    _maxLongitude = value
  }
  
  
  
  
  private def verifyLat(value:Double)={
    if (value < -90 || value > 90) 
      throw new IllegalArgumentException("Latitude values must be between -90 and 90 inclusive.")
  }
 
  private def verifyLong(value:Double)={
    if (value < -180 || value > 179.999999)
      throw new IllegalArgumentException("Longitude values must be between -180 and 179.999999 inclusive.")
  }
  
}
  
  
