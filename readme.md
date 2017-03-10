https://s3-us-west-1.amazonaws.com/edmundssparksubmitresults/topVehicles.html

https://github.com/justwjr/Edmunds-Car-Data-Pipeline-sdk-python/blob/master/images/ARCHITECTURE_DIAGRAM.png?raw=true

https://www.gliffy.com/go/publish/11854628

![](https://github.com/justwjr/Edmunds-Car-Data-Pipeline-sdk-python/blob/master/images/ARCHITECTURE_DIAGRAM.png?raw=true)

# Robustness and fault tolerance

  There are many machines working together in my pipeline's ecosystem.  The ones somewhat under my control are the EC2 instance to run kinesis firehose, the S3 buckets, the EMR spark clusters, and of course my local machine.  The raw json objects streaming in from Edmunds API go into S3, and consolidated DataFrame tables get stored as parquet files as a backup.
  The kinesis firehose is a bit delicate, because a python file is running continuously using nohup, and it makes an API call for a specific make-model every 60 minutes.  When there's an error, it will store the object as an error and continue.  The system could be improved by setting up alerts whenever some part of the pipeline goes down.

# Low latency reads and updates

  Reads are low latency because a complete .html file gets transferred to the local machine before invoking a boto connection to S3 to update the static website.  An update to the Spark DataFrames and SQL tables first requieres reading in all the files in the S3 bucket for the raw json objects which takes some time.
  The spark.read.json("s3a://edmundsvehicle/2017/*/*/*/*") functionality could be improved by implementing a fast update version that only reads in new objects that have been added to the bucket since the last update.  Occassionally though (at least once a year) the entire bucket would need to be reupdated since car manufactures come out with new models.

# Scalability

  Since we are using Amazon EC2 clusters, we could easily scale up the system by adding more machines or upgrading the class.  Thus, it would be able to maintain performance while handling increasing data load.  The main deterents to saclability are limited API calls and costs.

# Generalization

  Since the entirety of the vehicle data gets stored in third-normal-form tables, it's easy to pull data through standard SQL queries.  This facilitates a wide range of applications for financial management, analytics, and market research.  To further improve the app, historical data could be added, and if the limitations of the API calls were increased, I could also add dealer inventory info, images, and much more.

# Extensibility

  The system can be easily extended, the flowchart can be modified to add new features.  To further extend this application, we could gather data from of the prices and dealer inventory information to augment the vehicle data.  The main limitation though is that the exploratory tier has a maximum limit of 25 API calls per day.
  

# Ad hoc queries
  
  All the data gets stored into 3 SQL tables that satisfy third normal form (3NF).  Ad hoc queries can be ran by writing or editing the standard SQL select statements in spark SQL.
  If we wanted to be a little more user friendly and make connections to a database, we could set up a SQL server using a tool such as PG Admin to host the database server.

# Minimal maintenance

  The Kinesis firehose runs every hour on an EC2 instance automatically.  Errors get logged as objects, so they don't inturrupt the process.  To keep things simple, all the SQL requirements are done using Spark SQL and the results ouput to an html file.
  An update to the webpage requires running a program on a local machine to fetch the updated html file and push changes.

# Debuggability

  Errors get printed the log and also can get stored as objects, making things easy to debug.  It can be further improved by using a Lambda Architecture and functional batch layer, using recomputation algorithms when possible.
