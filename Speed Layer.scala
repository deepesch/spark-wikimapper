// Databricks notebook source exported at Tue, 8 Mar 2016 20:55:09 UTC
// MAGIC %md
// MAGIC 
// MAGIC #![Wikipedia Logo](http://sameerf-dbc-labs.s3-website-us-west-2.amazonaws.com/data/wikipedia/images/w_logo_for_labs.png)
// MAGIC 
// MAGIC # Wikipedia top editors

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

// MAGIC %md Create a new `StreamingContext`, using the SparkContext and batch interval:

// COMMAND ----------

val ssc = new StreamingContext(sc, BatchInterval)

// COMMAND ----------

// MAGIC %md ####Create one Dstream for English and another for a language

// COMMAND ----------

val baseEnDSTREAM = ssc.socketTextStream(EnglishStreamingServerHost, EnglishStreamingServerPort)

// COMMAND ----------

val baseDeDSTREAM = ssc.socketTextStream(MiscLangStreamingServerHost, GermanStreamingServerPort)

// COMMAND ----------

// MAGIC %md For each DStream, parse the incoming JSON and register a new temporary table every batch interval:

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

// MAGIC %sql select * from English_Edits

// COMMAND ----------

// MAGIC %sql select * from German_Edits

// COMMAND ----------

// MAGIC %sql select count(*) from English_Edits

// COMMAND ----------

// MAGIC %sql 
// MAGIC select "English" AS language, substring(timestamp, 0, 19) as timestamp, count(*) AS count from English_Edits GROUP BY timestamp UNION ALL
// MAGIC select "German" AS language, substring(timestamp, 0, 19) as timestamp, count(*) AS count from German_Edits GROUP BY timestamp; 

// COMMAND ----------

// MAGIC %md ###SQL Query to get most active users by filtering bots & anonymous users(IP Addresses) 

// COMMAND ----------

// MAGIC 
// MAGIC %sql 
// MAGIC 
// MAGIC SELECT 
// MAGIC user, count(user)
// MAGIC FROM English_edits 
// MAGIC WHERE user NOT REGEXP '[b|Bot|^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$]' 
// MAGIC GROUP BY user 
// MAGIC ORDER BY count(user) DESC
// MAGIC limit 10;

// COMMAND ----------



// COMMAND ----------

// MAGIC %md ###Export CSV

// COMMAND ----------

//stop
StreamingContext.getActive.foreach { _.stop(stopSparkContext = false) }
