// Databricks notebook source exported at Tue, 8 Mar 2016 20:55:09 UTC
// %md
// 
// #![Wikipedia Logo](http://sameerf-dbc-labs.s3-website-us-west-2.amazonaws.com/data/wikipedia/images/w_logo_for_labs.png)
// 
// # Wikipedia top editors

// COMMAND ----------

import org.apache.spark._
import org.apache.spark.storage._
import org.apache.spark.streaming._
import org.apache.spark.sql.functions._

// COMMAND ----------

// The batch interval 
val BatchInterval = Minutes(5)

// We'll use a unique server for the English edit stream
val EnglishStreamingServerHost  = "52.89.53.194"
val EnglishStreamingServerPort  = 9002 //en

// We'll use a unique server for german language edit stream
val MiscLangStreamingServerHost  = "54.68.10.240"
val GermanStreamingServerPort  = 9003 //de

// COMMAND ----------

sc

// COMMAND ----------

// %md Create a new `StreamingContext`, using the SparkContext and batch interval:

// COMMAND ----------

val ssc = new StreamingContext(sc, BatchInterval)

// COMMAND ----------

// %md ####Create one Dstream for English and another for a language

// COMMAND ----------

val baseEnDSTREAM = ssc.socketTextStream(EnglishStreamingServerHost, EnglishStreamingServerPort)

// COMMAND ----------

val baseDeDSTREAM = ssc.socketTextStream(MiscLangStreamingServerHost, GermanStreamingServerPort)

// COMMAND ----------

// %md For each DStream, parse the incoming JSON and register a new temporary table every batch interval:

// COMMAND ----------

// Create an English temp table at every given batch interval
baseEnDSTREAM.foreachRDD { rdd =>
  if(! rdd.isEmpty) {
    sqlContext.read.json(rdd).registerTempTable("English_Edits")
  }
}
// Create an German temp table at every given batch interval
  baseDeDSTREAM.foreachRDD { rdd => 
    
    if (! rdd.isEmpty) {
      sqlContext.read.json(rdd).registerTempTable("German_Edits")
    }
  }


// COMMAND ----------

  ssc.remember(Minutes(10))  // To make sure data is not deleted by the time we query it interactively

// COMMAND ----------

ssc.start

// COMMAND ----------

// %sql select * from English_Edits

// COMMAND ----------

// %sql select * from German_Edits

// COMMAND ----------

// %sql select count(*) from English_Edits

// COMMAND ----------

// %sql 
// select "English" AS language, substring(timestamp, 0, 19) as timestamp, count(*) AS count from English_Edits GROUP BY timestamp UNION ALL
// select "German" AS language, substring(timestamp, 0, 19) as timestamp, count(*) AS count from German_Edits GROUP BY timestamp; 

// COMMAND ----------

// %md ###SQL Query to get most active users by filtering bots & anonymous users(IP Addresses) 

// COMMAND ----------

// 
// %sql 
// 
// SELECT 
// user, count(user)
// FROM English_edits 
// WHERE user NOT REGEXP '[b|Bot|^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$]' 
// GROUP BY user 
// ORDER BY count(user) DESC
// limit 10;

// COMMAND ----------



// COMMAND ----------

// %md ###Export CSV

// COMMAND ----------

//stop
StreamingContext.getActive.foreach { _.stop(stopSparkContext = false) }
