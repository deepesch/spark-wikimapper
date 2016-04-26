// Databricks notebook source exported at Tue, 8 Mar 2016 20:55:29 UTC
// MAGIC %md
// MAGIC 
// MAGIC #![Wikipedia Logo](http://sameerf-dbc-labs.s3-website-us-west-2.amazonaws.com/data/wikipedia/images/w_logo_for_labs.png)
// MAGIC 
// MAGIC # Wikipedia Redlinks

// COMMAND ----------

// MAGIC %fs ls /databricks-datasets/wikipedia-datasets/data-001/clickstream/raw-uncompressed

// COMMAND ----------

// MAGIC %md Size of 1322171548 bytes means 1.2 GB.

// COMMAND ----------

// MAGIC %md
// MAGIC ### DataFrames
// MAGIC Let's use the `sqlContext` to read a tab seperated values file (TSV) of the Clickstream data.

// COMMAND ----------

// Notice that the sqlContext is actually a HiveContext
sqlContext

// COMMAND ----------

//Create a DataFrame with the anticipated structure
val DF = sqlContext.read.format("com.databricks.spark.csv")
  .option("header", "true")
  .option("delimiter", "\\t")
  .option("mode", "PERMISSIVE")
  .option("inferSchema", "true")
  .load("dbfs:///databricks-datasets/wikipedia-datasets/data-001/clickstream/raw-uncompressed")

// COMMAND ----------

// MAGIC %md Note that it took 19.15s minute to read the 1.2 GB file from S3. The above cell kicked off 2 Spark jobs, the first job has one task and just infers the schema from the file. The 2nd job uses 20 tasks to read the 1.2 GB file in parallel (each task reads about 64 MB of the file). 

// COMMAND ----------

display(DF)

// COMMAND ----------

// MAGIC %md ####22 MIllion requests(Out of 3.2 Billion) 

// COMMAND ----------

// MAGIC %md ###Schema and Data Types 

// COMMAND ----------

clickstreamDF.printSchema()

// COMMAND ----------

// MAGIC %md New DataFrame without Prev_id and curr_id:

// COMMAND ----------

// New DataFrame without Prev_id and curr_id:
val DF2 = clickstreamDF.select($"prev_title", $"curr_title", $"n", $"type")

// COMMAND ----------

// MAGIC %md Description of columns:
// MAGIC 
// MAGIC - `prev_title`: the result of mapping the referer URL to the fixed set of values described above
// MAGIC 
// MAGIC - `curr_title`: the title of the article the client requested
// MAGIC 
// MAGIC - `n`: the number of occurrences of the (referer, resource) pair
// MAGIC 
// MAGIC - `type`
// MAGIC   - "link" if the referer and request are both articles and the referer links to the request
// MAGIC   - "redlink" if the referer is an article and links to the request, but the request is not in the production enwiki.page table
// MAGIC   - "other" if the referer and request are both articles but the referer does not link to the request. This can happen when clients search or spoof their refer

// COMMAND ----------

// MAGIC %md Reading from disk vs memory, The 1.2 GB Clickstream file is currently on S3, which means each time you scan through it, your Spark cluster has to read the 1.2 GB of data remotely over the network.

// COMMAND ----------

// MAGIC %md Lazily cache the data and then call the `count()` action to check how many rows are in the DataFrame and to see how long it takes to read the DataFrame from S3:

// COMMAND ----------

// cache() is a lazy operation, so we need to call an action (like count) to materialize the cache
DF2.cache().count()

// COMMAND ----------

// MAGIC %md So it takes about 18 seconds to read the 1.2 GB file into Spark cluster. The file has 22.5 million rows/lines.

// COMMAND ----------

DF2.count()

// COMMAND ----------

// MAGIC %md Same operation takes 0.61s after catching

// COMMAND ----------

// MAGIC %md Spark's in-memory columnar compression helps to reduce size of data by 1/3

// COMMAND ----------

// MAGIC %sql SET spark.sql.inMemoryColumnarStorage.compressed

// COMMAND ----------

// MAGIC %md We start by grouping by the current title and summing the number of occurrances of the current title:

// COMMAND ----------

display(DF2.groupBy("curr_title").sum().limit(10))

// COMMAND ----------

// MAGIC %md ###Spark SQL

// COMMAND ----------

//First register the table, so we can call it from SQL
DF2.registerTempTable("clickstream")

// COMMAND ----------

// MAGIC %sql SELECT * FROM clickstream LIMIT 5;

// COMMAND ----------

// MAGIC %md DataFrames query to SQL:

// COMMAND ----------

// MAGIC %sql SELECT curr_title, SUM(n) AS top_articles FROM clickstream GROUP BY curr_title ORDER BY top_articles DESC LIMIT 10;

// COMMAND ----------

// MAGIC %md Spark SQL is typically used for batch analysis of data,It is not designed to be a low-latency transactional database like cassandra, INSERTs, UPDATEs and DELETEs are not supported.

// COMMAND ----------

display(clickstreamDF2.groupBy("prev_title").sum().orderBy($"sum(n)".desc).limit(10))

// COMMAND ----------

// MAGIC %md 3.2 billion requests total for English Wikipedia pages

// COMMAND ----------

// MAGIC %md Import spark statistical functions

// COMMAND ----------

// Import the sql statistical functions like sum, max, min, avg
import org.apache.spark.sql.functions._

// COMMAND ----------

// MAGIC %md 
// MAGIC ### 
// MAGIC ** Most requested missing pages? ** (These are the articles that our top users should create on Wikipedia!)

// COMMAND ----------

// MAGIC %md The type column of our table has 3 possible values:

// COMMAND ----------

// MAGIC %sql SELECT DISTINCT type FROM clickstream;

// COMMAND ----------

// MAGIC %md These are described as:
// MAGIC   - **link** - if the referer and request are both articles and the referer links to the request
// MAGIC   - **redlink** - if the referer is an article and links to the request, but the request is not in the production enwiki.page table
// MAGIC   - **other** - if the referer and request are both articles but the referer does not link to the request. This can happen when clients search or spoof their refer

// COMMAND ----------

// MAGIC %md Redlinks are links to a Wikipedia page that does not exist, either because it has been deleted, or because the author is anticipating the creation of the page. Seeing which redlinks are the most viewed is interesting because it gives some indication about demand for missing content.
// MAGIC 
// MAGIC Let's find the most popular redlinks:

// COMMAND ----------

display(DF2.filter("type = 'redlink'").groupBy("curr_title").sum().orderBy($"sum(n)".desc).limit(10))

// COMMAND ----------

// MAGIC %md Indeed there doesn't appear to be an article on the [2027_Cricket_World_Cup](https://en.wikipedia.org/wiki/2027_Cricket_World_Cup) on Wikipedia. We will use these to redirect our users
// MAGIC 
// MAGIC Note that if you clicked on the link for 2027_Cricket_World_Cup in this cell, then you registered another Redlink for that article.

// COMMAND ----------

val redlink_df = DF2.filter("type = 'redlink'").groupBy("curr_title").sum().orderBy($"sum(n)".desc).limit(100)

// COMMAND ----------

redlink_df.write.format("com.databricks.spark.csv").save("/workbook.csv")

// COMMAND ----------

// MAGIC %md Join the two DataFrames 

// COMMAND ----------

val in_outDF = pageviewsPerArticleDF.join(linkclicksPerArticleDF, ($"curr_title" === $"prev_title")).orderBy($"in_count".desc)

in_outDF.show(10)

// COMMAND ----------

// MAGIC %md The `curr_title` and `prev_title` above are the same, so we can just display one of them in the future. Next, add a new `ratio` column to easily see whether there is more `in_count` or `out_count` for an article:

// COMMAND ----------

val in_out_ratioDF = in_outDF.withColumn("ratio", $"out_count" / $"in_count").cache()

in_out_ratioDF.select($"curr_title", $"in_count", $"out_count", $"ratio").show(5)

// COMMAND ----------

// MAGIC %md We can see above that when clients went to the **Alive** article, almost nobody clicked any links in the article to go on to another article.
// MAGIC 
// MAGIC But 49% of people who visited the **Fifty Shades of Grey** article clicked on a link in the article and continued to browse Wikipedia.

// COMMAND ----------

// MAGIC %md 
// MAGIC ** Traffic flow pattern look like for the "San Francisco" article **

// COMMAND ----------

in_out_ratioDF.filter("curr_title = 'San_Francisco'").show()

// COMMAND ----------

// MAGIC %md "San Francisco" gets most traffic from Google

// COMMAND ----------

// MAGIC %sql SELECT * FROM clickstream WHERE curr_title LIKE 'San_Francisco' ORDER BY n DESC LIMIT 10;

// COMMAND ----------

// MAGIC %sql SELECT * FROM clickstream WHERE prev_title LIKE 'San_Francisco' ORDER BY n DESC LIMIT 10;

// COMMAND ----------

displayHTML("""
<!DOCTYPE html>
<body>
<script type="text/javascript"
           src="https://www.google.com/jsapi?autoload={'modules':[{'name':'visualization','version':'1.1','packages':['sankey']}]}">
</script>

<div id="sankey_multiple" style="width: 900px; height: 300px;"></div>

<script type="text/javascript">
google.setOnLoadCallback(drawChart);
   function drawChart() {
    var data = new google.visualization.DataTable();
    data.addColumn('string', 'From');
    data.addColumn('string', 'To');
    data.addColumn('number', 'Weight');
    data.addRows([
 ['other-google', 'San_Francisco', 28748],
 ['other-empty', 'San_Francisco', 6307],
 ['other-other', 'San_Francisco', 5405],
 ['other-wikipedia', 'San_Francisco', 3061],
 ['other-bing', 'San_Francisco', 1624],
 ['Main_Page', 'San_Francisco', 1479],
 ['California', 'San_Francisco', 1222],
 ['San_Francisco', 'List_of_people_from_San_Francisco', 1337],
 ['San_Francisco', 'Golden_Gate_Bridge', 852],
 ['San_Francisco', 'Oakland', 775],
 ['San_Francisco', 'Los_Angeles', 712],
 ['San_Francisco', '1906_San_Francisco_earthquake', 593],
 ['San_Francisco', 'Alcatraz_Island', 564],
 ['San_Francisco', 'Transamerica_Pyramid', 553],
    ]);
    // Set chart options
    var options = {
      width: 600,
      sankey: {
        link: { color: { fill: '#grey', fillOpacity: 0.3 } },
        node: { color: { fill: '#a61d4c' },
                label: { color: 'black' } },
      }
    };
    // Instantiate and draw our chart, passing in some options.
    var chart = new google.visualization.Sankey(document.getElementById('sankey_multiple'));
    chart.draw(data, options);
   }
</script>
  </body>
</html>""")

// COMMAND ----------

// MAGIC %md The chart above shows how people get to a Wikipedia article and what articles they click on next.
// MAGIC 
// MAGIC This diagram shows incoming traffic to the "San Francisco" article. We can see that most people found the "San Francisco"
