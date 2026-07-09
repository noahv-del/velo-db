from configparser import ConfigParser
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, HiveContext
import os

#ENVIRONMENT DIRECTORIES
#Change to the root path of you installation directories. (ex. c:\\tools\bigdatatools\spark-3.5.2-bin-hadoop3)
spark_home = f"C:{os.sep}tools{os.sep}bigdatatools{os.sep}spark-3.5.2-bin-hadoop3" # Change to own hadoop-folder. Use double slashes when needed. You can also use os.sep instead of the double slaches.
hadoop_home = f"C:{os.sep}tools{os.sep}bigdatatools{os.sep}hadoop-3.4.0-win10-x64" # For windows: use the hadoop folder in bigdatatools. For linux/mac: Try with provided hadoop folder in bigdatatools, if that doesn't work, download hadoop from the apache website: https://hadoop.apache.org/release/3.4.0.html
java_home = "C:"+ os.sep + "Program Files" + os.sep +"Java" +os.sep + "jdk-11"
#########################

# DO NOT CHANGE ANYTHING BELOW THIS LINE
#Configparser is a helper class to read properties from a configuration file
config = ConfigParser()
config.read('config.ini') #Define connection properties is the config file
cn = "default" #This is the default connection-name. Create a "default" profile in config.ini


#This function can be used to set systemvariables before running code. This eliminates the need to set the variables in the os.
def setupEnvironment():
    os.environ["PYSPARK_PYTHON"] = "python"
    os.environ["SPARK_HOME"] = spark_home
    os.environ["HADOOP_HOME"] = hadoop_home
    os.environ["PYSPARK_HADOOP_VERSION"] ="3"
    os.environ["JAVA_HOME"] = java_home + os.sep
    pathlist = [spark_home + os.sep + "bin", hadoop_home + os.sep +  "bin", java_home + os.sep + "bin"]
    os.environ["PATH"] += os.pathsep + os.pathsep.join(pathlist)

#This function can be used to list all environment variables.
def listEnvironment():
    import os
    for key, value in os.environ.items():
        print(f'{key}: {value}')

#This function can be used to start the sparkcluster on the local machine and return the sparksession.
def startLocalCluster(appName, partitions=4):
    builder = SparkSession.builder \
        .appName(appName) \
   .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.shuffle.partitions", partitions) \
        .config("spark.executor.extraJavaOptions",
                "-Dsasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=RHUIVNZYHMOODAKC password=/bkAQpKgCWjkPfTgxRY973VOhKf+MmwFulZLrAdrwdlXUTta0AUvNjo/8U57R8/w;") \
        .config("spark.executor.extraJavaOptions", "-Dsasl.mechanism=PLAIN") \
        .master("local[*]")

    extra_packages = ["org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0",get_Property("driver_repo"),"org.elasticsearch:elasticsearch-spark-30_2.12:8.15.2"]
  # These are the packages that are needed for the sparksession to work with kafka and sqlserver
    builder = configure_spark_with_delta_pip(builder, extra_packages=extra_packages) # This function adds the delta-lake package to the sparksession and adds the extra packages to all the executors.
    spark = builder.getOrCreate()
    return spark



def create_jdbc():
    return get_Property("url")


# Set the connectionName that has to be used (if you don't want to use the default profile
def set_connectionProfile(connectionName):
    global cn
    cn = connectionName

#Returns a specific property from the connection profile in the config.ini
def get_Property(propertyName):
  return config.get(cn, propertyName)