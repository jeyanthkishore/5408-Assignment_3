import re, string
import pymongo
import json
from pyspark import SparkContext

#Database Connectivity and Keywords declaration
myclient = pymongo.MongoClient('mongodb+srv://jeyanth:7HPAE8apzyvPmxdV@cluster0.smhz3.mongodb.net/test?authSource=admin&replicaSet=atlas-7myp9l-shard-0&readPreference=primary&appname=MongoDB%20Compass&ssl=true')
databasename=['ProcessedDb','ReuterDb']
search_keywords_list = ["canada","flu","snow","storm","indoor","safety","rain","ice","winter","cold","hot"]
spcl_char_regex=re.compile('[@_!#$%^&*\\n();,.\'<>?/\|}{~:]')

def performMapReduce(process_list,databasename):
     """This method performs MapReduce Operation.
    
    Returns:
      None.
    """
    sc = SparkContext.getOrCreate()
    rdd = sc.parallelize(process_list)
    frequency_count = rdd.flatMap(lambda x: x.split()).filter(lambda y: y in search_keywords_list)
    frequency_count = frequency_count.map(lambda word: (word, 1)).reduceByKey(lambda x,y: x + y)
    output_list = frequency_count.collect()
    print('MapReducer using Pyspark for the database : '+databasename)
    print(output_list)
    temp_list=[output[1] for output in output_list]
    maxindex = temp_list.index(max(temp_list))
    print("Maximum occured element is ({},{})".format(output_list[maxindex][0],output_list[maxindex][1]))
    minindex = temp_list.index(min(temp_list))
    print("Minimum occured element is ({},{})".format(output_list[minindex][0],output_list[minindex][1]))

#Fetching data from database
for process in databasename:
    process_list = []
    database = myclient[process]
    collectionname = database["processeddata"]
    for data in collectionname.find():
        lowercasedata = str(data).lower()
        process_data = re.sub(spcl_char_regex,"",lowercasedata)
        process_list.append(process_data)
    performMapReduce(process_list,process)