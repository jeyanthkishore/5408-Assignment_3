import re
import pymongo

#Database connectivity and file details
myclient = pymongo.MongoClient('mongodb+srv://jeyanth:7HPAE8apzyvPmxdV@cluster0.smhz3.mongodb.net/test?authSource=admin&replicaSet=atlas-7myp9l-shard-0&readPreference=primary&appname=MongoDB%20Compass&ssl=true')
database = myclient['ReuterDb']
collection = database['processeddata']
filename = ['reut2-009.sgm','reut2-014.sgm']
spcl_char_regex=re.compile('[@_!#$%^&*\\n();,.\'<>?/\|}{~:]')

#Accesing the files
for file in filename:
     with open(file,'r') as fh:
          newcollection = []
          filedata = fh.read()
          reg_str = "<REUTERS[^>]*>\s*((?:.|\n)*?)</REUTERS>"
          res = re.findall(reg_str, filedata)
          print('Processing of Data, start for the file '+file)
          #Individual news are obtained and processed below
          for line in res:
               reuter = {}
               title = ''
               try:
                    place = re.findall('<PLACES[^>]*>([^<]+)</PLACES>', line)[0]
               except:
                    place = ''
               try:
                    date = re.findall('<DATE[^>]*>([^<]+)</DATE>', line)[0]
               except:
                    date = ''
               try:
                    people = re.findall('<PEOPLE[^>]*>([^<]+)</PEOPLE>', line)[0]
               except:
                    people = ''
               try:
                    companies = re.findall('<COMPANIES[^>]*>([^<]+)</COMPANIES>', line)[0]
               except:
                    companies = ''
               try:
                    orgs = re.findall('<ORGS[^>]*>([^<]+)</ORGS>', line)[0]
               except:
                    orgs = ''
               try:
                    topics = re.findall('<TOPICS[^>]*>([^<]+)</TOPICS>', line)[0]
               except:
                    topics = ''
               #Texttag contains the title and data
               texttags = re.findall("<TEXT[^>]*>([\s\S]+[^<]*?)</TEXT>", line)
               for texttag in texttags:
                    titleText = re.findall("<TITLE[^>]*>([^<]+)</TITLE>", texttag)
                    for titleOne in titleText:
                         title = titleOne
                    result = re.sub('<TITLE[^>]*>([^<]+)</TITLE>', '', texttag)
                    body = re.sub('<.*?>', '', result)
                    body = re.sub(spcl_char_regex,'',body)
               reuter["title"] = title
               reuter["place"] = place
               reuter["date"] = date
               reuter["topics"] = topics
               reuter["place"] = place
               reuter["people"] = people
               reuter["orgs"] = orgs
               reuter["companies"] = companies
               reuter["data"] = body
               newcollection.append(reuter)
          print('Data of the file '+file+' is saved to DB')
          collection.insert_many(newcollection)