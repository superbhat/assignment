package weatther.app
import org.apache.spark._
import org.apache.spark
import org.apache.spark.sql
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col,column}
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.functions.{col,column}
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.{StringType,LongType, DoubleType}

//FOLLOWING CODE IS WRIITEN IN SCALA, PERAPRES WEATHER DATA FOR 11 MAIN CITIES OF AUSTRALIA.
//SOURCE OF DATA BEING FROM BUREAU OF METEOROLOGY
object weather {
 def main(args:Array[String]){

//PREPARE SPARK SESSION   
   val spark: SparkSession = SparkSession.builder().appName("WEATHER APP")
   .config("spark.master", "local").config("spark.driver.allowMultipleContexts", "true").getOrCreate()
  
//GEOGRAPHY DETAILS OF 11 CITIES
   val dataf = "data/data1.csv"

//ATMOSPHERIC DETAILS OF 11 CITIES   
   val brisf = "data/brisbane.csv"
   val adelf = "data/Adelaide.csv"
   val bromf = "data/broome.csv"
   val canbf = "data/canberra.csv"
   val darwf = "data/darwin.csv"
   val hobaf = "data/hobart.csv"
   val newcf = "data/newcastle.csv"
   val pertf = "data/perth.csv"
   val sydnf = "data/sydney.csv"
   val townf = "data/townsville.csv"
   val wollf = "data/wollongong.csv"

//SCHEMA FOR GEOGRAPHY DATAFRAME   
   val schema1 = new StructType(Array(
       new StructField("City_Name", StringType, true),
       new StructField("Latitude", DoubleType, true),
       new StructField("Longitude", DoubleType, true),
       new StructField("Max_Temp", DoubleType, true),
       new StructField("Min_Temp", DoubleType, true),
       new StructField("Altitude", DoubleType, true)))

//SCHEMA FOR ATMOSPHERIC DATAFRAME
   val schema2 = new StructType(Array(
       new StructField("Date_Time", StringType, true),
       new StructField("Humidity", DoubleType, true),
       new StructField("Pressure", DoubleType, true)))
   
//GEOGRAPHY DATAFRAME OF 11 CITIES  
   val data_fr = spark.read.format("csv").option("header","false").schema(schema1).load(dataf)
   
//CITIES ATOMOSPHERIC DATAFRAME    
   val adel_fr = spark.read.format("csv").option("header","false").schema(schema2).load(adelf)
   val bris_fr = spark.read.format("csv").option("header","false").schema(schema2).load(brisf)
   val brom_fr = spark.read.format("csv").option("header","false").schema(schema2).load(bromf)
   val canb_fr = spark.read.format("csv").option("header","false").schema(schema2).load(canbf)
   val darw_fr = spark.read.format("csv").option("header","false").schema(schema2).load(darwf)
   val hoba_fr = spark.read.format("csv").option("header","false").schema(schema2).load(hobaf)
   val newc_fr = spark.read.format("csv").option("header","false").schema(schema2).load(newcf)
   val pert_fr = spark.read.format("csv").option("header","false").schema(schema2).load(pertf)
   val sydn_fr = spark.read.format("csv").option("header","false").schema(schema2).load(sydnf)
   val town_fr = spark.read.format("csv").option("header","false").schema(schema2).load(townf)
   val woll_fr = spark.read.format("csv").option("header","false").schema(schema2).load(wollf)
   
//RELATIVE HUMIDITY CALCULATION.
   val hum_adl   = adel_fr.select(avg(col("Humidity"))).first()(0).toString.toDouble
   val hum_bri   = bris_fr.select(avg(col("Humidity"))).first()(0).toString.toDouble
   val hum_bro   = brom_fr.select(avg(col("Humidity"))).first()(0).toString.toDouble
   val hum_can   = canb_fr.select(avg(col("Humidity"))).first()(0).toString.toDouble
   val hum_dar   = darw_fr.select(avg(col("Humidity"))).first()(0).toString.toDouble
   val hum_hob   = hoba_fr.select(avg(col("Humidity"))).first()(0).toString.toDouble
   val hum_new   = newc_fr.select(avg(col("Humidity"))).first()(0).toString.toDouble
   val hum_per   = pert_fr.select(avg(col("Humidity"))).first()(0).toString.toDouble
   val hum_syd   = sydn_fr.select(avg(col("Humidity"))).first()(0).toString.toDouble
   val hum_tow   = town_fr.select(avg(col("Humidity"))).first()(0).toString.toDouble
   val hum_wol   = woll_fr.select(avg(col("Humidity"))).first()(0).toString.toDouble
   
//PRESSURE IN HPA FOR 11 CITIES.   
   val press_adl = adel_fr.select(avg(col("Pressure"))).first()(0).toString.toDouble
   val press_bri = bris_fr.select(avg(col("Pressure"))).first()(0).toString.toDouble
   val press_bro = brom_fr.select(avg(col("Pressure"))).first()(0).toString.toDouble
   val press_can = canb_fr.select(avg(col("Pressure"))).first()(0).toString.toDouble
   val press_dar = darw_fr.select(avg(col("Pressure"))).first()(0).toString.toDouble
   val press_hob = hoba_fr.select(avg(col("Pressure"))).first()(0).toString.toDouble
   val press_new = newc_fr.select(avg(col("Pressure"))).first()(0).toString.toDouble
   val press_per = pert_fr.select(avg(col("Pressure"))).first()(0).toString.toDouble
   val press_syd = sydn_fr.select(avg(col("Pressure"))).first()(0).toString.toDouble
   val press_tow = town_fr.select(avg(col("Pressure"))).first()(0).toString.toDouble
   val press_wol = woll_fr.select(avg(col("Pressure"))).first()(0).toString.toDouble
   
//IMPORTING IMPLICITS PACKAGE
   import spark.implicits._

//RDD TO DATAFRAME HAVING RH AND PRESS DETAILS   
   val rdd_1 = List(("SYDNEY",hum_syd,press_syd),("ADELAIDE",hum_adl,press_adl),
             ("BRISBANE",hum_bri,press_bri),("BROOME",hum_bro,press_bro),("CANBERRA",hum_can,press_can),
             ("DARWIN",hum_dar,press_dar),("HOBART",hum_hob,press_hob),("NEWCASTLE",hum_new,press_new),
             ("PERTH",hum_per,press_per),("TOWNSVILLE",hum_tow,press_tow),
             ("WOLLONGONG",hum_wol,press_wol))
   val ct_rh_ph = spark.createDataFrame(rdd_1).toDF("City_Name","Humidity","Pressure")
   
//RDD TO DATAFRAME LOCAL TIME DETAILS
   val dt_timedf1 = List(("SYDNEY","2018-06-20T11:10:29Z"),("ADELAIDE","2018-06-19T12:30:12Z"),("BRISBANE","2018-06-20T18:06:30Z"),("BROOME","2018-06-19T07:45:02Z"),
                 ("CANBERRA","2018-06-21T21:09:50Z"),("DARWIN","2018-06-20T06:05:04Z"),("HOBART","2018-06-21T20:04:45Z"),
                 ("NEWCASTLE","2018-06-18T17:45:58Z"),("PERTH","2018-06-20T14:27:28Z"),("TOWNSVILLE","2018-06-18T16:04:09Z"),("WOLLONGONG","2018-06-21T10:02:00Z"))
   val dt_timedf = spark.createDataFrame(dt_timedf1).toDF("City_Name","Local Time")               
                 
//UDF FOR CONCANTINATING LOCATION DETAILS LATITUDE LONGITTUDE ALTITUDE
  val concatudf = udf((col1:Double,col2:Double,col3:Double) => (col1+"_"+col2+"_"+col3))
  
//UDF FOR CONDITION LOGIC - 
//IF HUMIDITY IS MORE THAN 60% WAS A RAINY DAY 
//TEMPERATURE IS LESS THAN OR EQUAL TO 0 DEGREE IT WILL SNOW
//TEMPERATURE MORE THAN 0 AND HUMIDITY LESS THAN 60 IT'S A SUNNY DAY  
  val condudf = udf((col1:Double,col2:Double) => {if (col2>60) ("Rain") else {if (col1<=0 ) ("Snow") else ("Sunny")}})
  
//UDF FOR CALCULATING AVERAGE TEMPERATURE FOR A DAY  
  val tempudf = udf((col1:Double,col2:Double) => (col1 + col2)/2)
  
//JOINING DATAFRAMES GEOGRAPHY AND ATMOSPHERIC AND LOCAL TIME
  val join1 = data_fr.join(ct_rh_ph,"City_Name")
  val intr_df = join1.join(dt_timedf,"City_Name")
  
//EXTRACTING IATA_CODE FROM THE CITY_NAME
  val df_1 = intr_df.withColumn("IATA_Code",substring(intr_df("City_Name"),1,3))
  
//DERIVING LOCATION, TEMPERATURE AND CONDITIONS LOGIC USING UDF'S  
  val df_2 = df_1.withColumn("Location", concatudf(col("Latitude"),col("Longitude"),col("Altitude")))
  val df_3 = df_2.withColumn("Temperature", tempudf(col("Max_Temp"),col("Min_Temp")))
  val df_4 = df_3.withColumn("Conditions", condudf(col("Temperature"),col("Humidity")))
  
  
//PREPARING DATAFRAME WITH DESRIED COLUMNS
  val intr_df_2 = df_4.select(col("IATA_Code"),col("Location"),col("Local Time"),col("Conditions"),col("Temperature"),col("Pressure"),col("Humidity"))

//CONVERTING DATAFRAME TO ROW OF STRING
  val df_str = intr_df_2.collect().map { row => row.toString() }
   
//FORMATTING DATA AS PER OUTPUT   
  val df_out = df_str.map( x => x.replace(",", "|")).toList
  val out = df_out.map( x => x.replace("_", ",")).toList
  
//DISPLAYING THE OUTPUT
  val out_put = out.foreach(println)
  print(out_put)
 }
}