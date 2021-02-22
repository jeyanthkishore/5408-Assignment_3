import tweepy
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
import time
import pymongo
import re

record = 0
tweetlist = []
StreamList = ['Flu','Snow','Canada','Winter']
SerachList = ['Temperature','Indoor','Storm','Safety']
CombinedList = ['Flu','Snow','Canada','Temperature','Storm','Indoor','Safety','Winter']
to_remove_keys=["retweet_count","in_reply_to_status_id_str","in_reply_to_user_id","in_reply_to_user_id_str"
,"in_reply_to_screen_name","quote_count","favourite_count","retweeted","user","entities",
"quoted_status","extended_tweet","extended_entities","is_quote_status","retweeted_status","contributors","truncated"]
user_keys=["name","description","location","created_at","id"]

#Listener Class to fetch live data
class StdOutListener(StreamListener):

    def __init__(self, time_limit=300):
        self.start_time = time.time()
        self.limit = time_limit
        global tweetlist
        tweetlist = []
        print('Listener is Initialized')
        super(StdOutListener, self).__init__()

    def on_data(self, raw_data): 
        data = json.loads(raw_data)
        global tweetlist
        tweetlist.append(data)
        if (time.time() - self.start_time) > self.limit:
            global record
            database(tweetlist,StreamList[record])
            print('Data Successfully saved to DB for the tweet')
            print(time.time(), self.start_time, self.limit)
            return False
        

    def on_error(self, status):
        print(status) 
        return False

#Authentication and Database Connectivity
auth = tweepy.OAuthHandler("ZUxBrbfz40Obx1bkVZO2NFgkG","Ny14QiUIhqKP9l8vdNwnw3gKCzk3kBgdkx7tUOpS42Wlq8vn52")
auth.set_access_token("133789794-h3BrGq8pmkRJF4FHI8u0QkgrSW2tUWK1aKcRCagc","0h7e8jY5GPBwM6gX9kb4YxV48dwf0z9tQ92xaw0bAI5rK")
api = tweepy.API(auth)
myclient = pymongo.MongoClient('mongodb+srv://jeyanth:7HPAE8apzyvPmxdV@cluster0.smhz3.mongodb.net/test?authSource=admin&replicaSet=atlas-7myp9l-shard-0&readPreference=primary&appname=MongoDB%20Compass&ssl=true')
rawdatabase = myclient['RawDb']
processeddatabase = myclient['ProcessedDb']

def database(data,list_name):
     """This method performs operations to store data into raw database.
    
    Returns:
      None.
    """
    rawcollection = rawdatabase[list_name]
    print(list_name)
    rawcollection.insert_many(data)
    print(myclient.list_database_names())

def dataCleaning():
    """This method performs operations like removing unwanted keys, url keys.
    
    Returns:
      None.
    """
    for i in range(len(post_process_list)):       
        for i_key in list(post_process_list[i]):
            if "url" in i_key:
                del post_process_list[i][i_key]                  
            if type(post_process_list[i][i_key]) is dict:                
                for key in list(post_process_list[i][i_key]):
                    if "url" in key:                        
                        del post_process_list[i][i_key][key]             
            if i_key in to_remove_keys:
                if i_key == "retweeted_status":
                    try:
                        del post_process_list[i][i_key]["extended_tweet"]                        
                    except KeyError:
                        pass
                elif i_key == "user":
                    try:
                        for userkey in list(post_process_list[i][i_key]):
                            if not(userkey) in user_keys:
                                del post_process_list[i][i_key][userkey]
                                continue
                    except:
                        pass
                else:       
                    try:
                        del post_process_list[i][i_key]                        
                    except KeyError:
                        pass   

def dataTransformation():
    """This method performs operations like removing special characters, 
    URL and Emoticons.
    
    Returns:
      None.
    """
    spcl_char_regex=re.compile('[@_!#$%^&*()<>?/\|}{~:]')
    url_regex = re.compile("http.*")    
    for main_index in range(len(post_process_list)):
        for i in list(post_process_list[main_index]):            
            if type(post_process_list[main_index][i]) is dict:               
                for k in list(post_process_list[main_index][i]):
                    try:                        
                        if type(k) is str:                           
                           deEmojify(post_process_list[main_index][i][k],post_process_list,main_index,i,k)                                
                           if re.search("<a", post_process_list[main_index][i][k]):                                
                                del post_process_list[main_index][i][k] 
                                continue                                      
                           a=re.search(url_regex,post_process_list[main_index][i][k])
                           if a:
                               post_process_list[main_index][i][k] = \
                                         post_process_list[main_index][i][k].replace(a.group(0),"")                           
                           if re.sub(spcl_char_regex,"",post_process_list[main_index][i]):
                               post_process_list[main_index][i]  = \
                                   re.sub(spcl_char_regex,"",post_process_list[main_index][i])
                    except:
                        pass                
            elif type(post_process_list[main_index][i]) is str and \
                     re.search("<a",post_process_list[main_index][i]):                    
                    del post_process_list[main_index][i]
                    continue
            elif type(post_process_list[main_index][i]) is str:
                    a=re.search(url_regex,post_process_list[main_index][i])
                    try:
                        post_process_list[main_index][i]=post_process_list[main_index][i].replace(a.group(0),"")
                    except:
                        pass                                     
                    deEmojify(post_process_list[main_index][i],post_process_list,main_index,i,"")
                    if re.sub(spcl_char_regex,"",post_process_list[main_index][i]):
                        post_process_list[main_index][i] = \
                            re.sub(spcl_char_regex,"",post_process_list[main_index][i])                                 
            else:
                pass    

def deEmojify(text,post_process_list,main_index,i,k):
    """This method removes all types of emoticons.
    
    Returns:
      None.
    """    
    regrex_pattern = re.compile(pattern = "["
        u"\U0001F600-\U0001F64F"
        u"\U0001F300-\U0001F5FF"
        u"\U0001F680-\U0001F6FF"
        u"\U0001F1E0-\U0001F1FF"
        u"\U00002702-\U000027B0"
        u"\U000024C2-\U0001F251"
        u"\U0001f926-\U0001f937"
        u"\U00010000-\U0010ffff"
        u"\u2640-\u2642" 
        u"\u2600-\u2B55"
        u"\u200d"
        u"\u23cf"
        u"\u23e9"
        u"\u231a"
        u"\ufe0f" 
        u"\u3030"
                           "]+", flags = re.UNICODE)    
    if(not k):
        if re.sub(regrex_pattern,'',post_process_list[main_index][i]):
            post_process_list[main_index][i]=re.sub(regrex_pattern,'',post_process_list[main_index][i])
    else:
        if re.sub(regrex_pattern,'',post_process_list[main_index][i][k]):
            post_process_list[main_index][i][k]=re.sub(regrex_pattern,'',post_process_list[main_index][i][k])

#Perform Stream Listener for fetching Tweets            
for x in StreamList:
    listener = StdOutListener()
    stream = Stream(auth, listener)
    print('Stream Listener is set for '+x)
    stream.filter(track=[x])
    record=record+1

#Perform Search Query for fetching Tweets
for x in SerachList:
    tweets = []
    print('Search Based query for '+x+' Tweets')
    tweets += tweepy.Cursor(api.search, q=x, lang="en",  tweet_mode='extended').items(300)
    tweetCollection = []
    for tweet in tweets:
        tweetCollection.append(tweet._json)
    rawcollection = rawdatabase[x]
    rawcollection.insert_many(tweetCollection)
    print('Save operation Complete for search of '+x)

#Perform processing of Tweets from RawDb
for x in CombinedList:
    print(x)
    rawcol = rawdatabase[x]
    post_process_list = []
    for data in rawcol.find():
        post_process_list.append(data)
    print('Performing Data Cleaning for '+x)
    dataCleaning()
    print('Performing Data Transformation for '+x)
    dataTransformation()
    processcollection = processeddatabase['processeddata']
    processcollection.insert_many(post_process_list)
    print('Processed Data stored in DB for '+x)
