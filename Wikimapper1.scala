// Databricks notebook source exported at Tue, 8 Mar 2016 20:55:29 UTC
// %md
// 
// #![Wikipedia Logo](http://sameerf-dbc-labs.s3-website-us-west-2.amazonaws.com/data/wikipedia/images/w_logo_for_labs.png)
// 
// # Wikipedia Redlinks

// COMMAND ----------

// %fs ls /databricks-datasets/wikipedia-datasets/data-001/clickstream/raw-uncompressed

// COMMAND ----------

// %md Size of 1322171548 bytes means 1.2 GB.

// COMMAND ----------

// %md
// ### DataFrames
// Let's use the `sqlContext` to read a tab seperated values file (TSV) of the Clickstream data.

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

// %md Note that it took 19.15s minute to read the 1.2 GB file from S3. The above cell kicked off 2 Spark jobs, the first job has one task and just infers the schema from the file. The 2nd job uses 20 tasks to read the 1.2 GB file in parallel (each task reads about 64 MB of the file). 

// COMMAND ----------

display(DF)

// COMMAND ----------

// %md ####22 MIllion requests(Out of 3.2 Billion) 

// COMMAND ----------

// %md ###Schema and Data Types 

// COMMAND ----------

clickstreamDF.printSchema()

// COMMAND ----------

// %md New DataFrame without Prev_id and curr_id:

// COMMAND ----------

// New DataFrame without Prev_id and curr_id:
val DF2 = clickstreamDF.select($"prev_title", $"curr_title", $"n", $"type")

// COMMAND ----------

// %md Description of columns:
// 
// - `prev_title`: the result of mapping the referer URL to the fixed set of values described above
// 
// - `curr_title`: the title of the article the client requested
// 
// - `n`: the number of occurrences of the (referer, resource) pair
// 
// - `type`
//   - "link" if the referer and request are both articles and the referer links to the request
//   - "redlink" if the referer is an article and links to the request, but the request is not in the production enwiki.page table
//   - "other" if the referer and request are both articles but the referer does not link to the request. This can happen when clients search or spoof their refer

// COMMAND ----------

// %md Reading from disk vs memory, The 1.2 GB Clickstream file is currently on S3, which means each time you scan through it, your Spark cluster has to read the 1.2 GB of data remotely over the network.

// COMMAND ----------

// %md Lazily cache the data and then call the `count()` action to check how many rows are in the DataFrame and to see how long it takes to read the DataFrame from S3:

// COMMAND ----------

// cache() is a lazy operation, so we need to call an action (like count) to materialize the cache
DF2.cache().count()

// COMMAND ----------

// %md So it takes about 18 seconds to read the 1.2 GB file into Spark cluster. The file has 22.5 million rows/lines.

// COMMAND ----------

DF2.count()

// COMMAND ----------

// %md Same operation takes 0.61s after catching

// COMMAND ----------

// %md Spark's in-memory columnar compression helps to reduce size of data by 1/3

// COMMAND ----------

// %sql SET spark.sql.inMemoryColumnarStorage.compressed

// COMMAND ----------

// %md We start by grouping by the current title and summing the number of occurrances of the current title:

// COMMAND ----------

display(DF2.groupBy("curr_title").sum().limit(10))

// COMMAND ----------

// %md ###Spark SQL

// COMMAND ----------

//First register the table, so we can call it from SQL
DF2.registerTempTable("clickstream")

// COMMAND ----------

// %sql SELECT * FROM clickstream LIMIT 5;

// COMMAND ----------

// %md DataFrames query to SQL:

// COMMAND ----------

// %sql SELECT curr_title, SUM(n) AS top_articles FROM clickstream GROUP BY curr_title ORDER BY top_articles DESC LIMIT 10;

// COMMAND ----------

// %md Spark SQL is typically used for batch analysis of data,It is not designed to be a low-latency transactional database like cassandra, INSERTs, UPDATEs and DELETEs are not supported.

// COMMAND ----------

display(clickstreamDF2.groupBy("prev_title").sum().orderBy($"sum(n)".desc).limit(10))

// COMMAND ----------

// %md 3.2 billion requests total for English Wikipedia pages

// COMMAND ----------

// %md Import spark statistical functions

// COMMAND ----------

// Import the sql statistical functions like sum, max, min, avg
import org.apache.spark.sql.functions._

// COMMAND ----------

// %md 
// ### 
// ** Most requested missing pages? ** (These are the articles that our top users should create on Wikipedia!)

// COMMAND ----------

// %md The type column of our table has 3 possible values:

// COMMAND ----------

// %sql SELECT DISTINCT type FROM clickstream;

// COMMAND ----------

// %md These are described as:
//   - **link** - if the referer and request are both articles and the referer links to the request
//   - **redlink** - if the referer is an article and links to the request, but the request is not in the production enwiki.page table
//   - **other** - if the referer and request are both articles but the referer does not link to the request. This can happen when clients search or spoof their refer

// COMMAND ----------

// %md Redlinks are links to a Wikipedia page that does not exist, either because it has been deleted, or because the author is anticipating the creation of the page. Seeing which redlinks are the most viewed is interesting because it gives some indication about demand for missing content.
// 
// Let's find the most popular redlinks:

// COMMAND ----------

display(DF2.filter("type = 'redlink'").groupBy("curr_title").sum().orderBy($"sum(n)".desc).limit(10))

// COMMAND ----------

// %md Indeed there doesn't appear to be an article on the [2027_Cricket_World_Cup](https://en.wikipedia.org/wiki/2027_Cricket_World_Cup) on Wikipedia. We will use these to redirect our users
// 
// Note that if you clicked on the link for 2027_Cricket_World_Cup in this cell, then you registered another Redlink for that article.

// COMMAND ----------

val redlink_df = DF2.filter("type = 'redlink'").groupBy("curr_title").sum().orderBy($"sum(n)".desc).limit(100)

// COMMAND ----------

redlink_df.write.format("com.databricks.spark.csv").save("/workbook.csv")

// COMMAND ----------

// %md Join the two DataFrames 

// COMMAND ----------

val in_outDF = pageviewsPerArticleDF.join(linkclicksPerArticleDF, ($"curr_title" === $"prev_title")).orderBy($"in_count".desc)

in_outDF.show(10)

// COMMAND ----------

// %md The `curr_title` and `prev_title` above are the same, so we can just display one of them in the future. Next, add a new `ratio` column to easily see whether there is more `in_count` or `out_count` for an article:

// COMMAND ----------

val in_out_ratioDF = in_outDF.withColumn("ratio", $"out_count" / $"in_count").cache()

in_out_ratioDF.select($"curr_title", $"in_count", $"out_count", $"ratio").show(5)

// COMMAND ----------

// %md We can see above that when clients went to the **Alive** article, almost nobody clicked any links in the article to go on to another article.
// 
// But 49% of people who visited the **Fifty Shades of Grey** article clicked on a link in the article and continued to browse Wikipedia.

// COMMAND ----------

// %md 
// ** Traffic flow pattern look like for the "San Francisco" article **

// COMMAND ----------

in_out_ratioDF.filter("curr_title = 'San_Francisco'").show()

// COMMAND ----------

// %md "San Francisco" gets most traffic from Google

// COMMAND ----------

// %sql SELECT * FROM clickstream WHERE curr_title LIKE 'San_Francisco' ORDER BY n DESC LIMIT 10;

// COMMAND ----------

// %sql SELECT * FROM clickstream WHERE prev_title LIKE 'San_Francisco' ORDER BY n DESC LIMIT 10;

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

// %md The chart above shows how people get to a Wikipedia article and what articles they click on next.
// 
// This diagram shows incoming traffic to the "San Francisco" article. We can see that most people found the "San Francisco"
