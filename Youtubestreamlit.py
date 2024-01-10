from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st

##API Insertion here

def Api_connect():
  Api_key="AIzaSyB74yz6vyHmYYvD-7dj6Yw3ZmoyHtYa58c"
  api_service_name = "youtube"
  api_version="v3"
  youtube=build(api_service_name, api_version, developerKey=Api_key)
  return youtube

youtube=Api_connect()

#to get channel information
def get_channel_info(channel_id):
  request=youtube.channels().list(
      part="snippet, ContentDetails, statistics",
      id=channel_id
  )
  response=request.execute()

  for i in response ["items"]:
    data=dict(Channel_Name=i["snippet"]["title"],
              Channel_Id=i["id"],
              Subscribers=i["statistics"]["subscriberCount"],
              Views=i["statistics"]["viewCount"],
              Total_Videos=i["statistics"]["videoCount"],
              Channel_Description=i["snippet"]["description"],
              Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"])
    return data
  
#to get the video Id's
def get_videos_ids(channel_id):
  video_ids=[]
  response=youtube.channels().list(id=channel_id, part="contentDetails").execute()
  Playlist_Id=response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
  next_page_token=None

  while True:
    response1=youtube.playlistItems().list(part="snippet",
                                           playlistId=Playlist_Id,
                                           maxResults=50,
                                           pageToken=next_page_token).execute()
    for i in range(len(response1["items"])):
      video_ids.append(response1["items"][i]["snippet"]["resourceId"]["videoId"])
    next_page_token=response1.get("nextPageToken")

    if next_page_token is None:
      break
  return video_ids

#get video information
def get_video_info(video_ids):
  video_data=[] #comment was made at the end
  for video_id in video_ids:
    request=youtube.videos().list(
        part="snippet, ContentDetails, statistics",
        id=video_id
    )
    response=request.execute()
    for item in response["items"]:
      data=dict(Channel_Name=item["snippet"]["channelTitle"],
                Channel_Id=item["snippet"]["channelId"],
                Video_Id=item["id"],
                Title=item["snippet"]["title"],         #because no tags in this channel
                Tags=item["snippet"].get("tags"),
                Thumbnail=item["snippet"]["thumbnails"]["default"]["url"],
                Description=item["snippet"]["description"],
                Published_Date=item["snippet"]["publishedAt"],
                Duration=item["contentDetails"]["duration"],
                views=item["statistics"]["viewCount"],
                Likes=item["statistics"].get("likeCount"),
                Comments=item["statistics"].get("commentCount"),
                Favorite_Count=item["statistics"]["favoriteCount"],
                Definition=item["contentDetails"]["definition"],
                Caption_status=item["contentDetails"]["caption"]
                )
      video_data.append(data)
  return video_data


#get comment information
def get_comment_info(video_ids):
  Comment_data=[]
  try: #because some of comments have been disabled
    for video_id in video_ids:
      request=youtube.commentThreads().list(
          part="snippet",
          videoId=video_id,
          maxResults=50
      )
      response=request.execute()

      for item in response["items"]:
        data=dict(Comment_Id=item["snippet"]["topLevelComment"]["id"],
                  Video_Id=item["snippet"]["topLevelComment"]["snippet"]["videoId"],
                  Comment_Text=item["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                  Comment_Author=item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                  Comment_Published=item["snippet"]["topLevelComment"]["snippet"]["publishedAt"])
        Comment_data.append(data)
  except:
      pass # this pass shows no error, if the comments have also been disabled
  return Comment_data

#upload to mongoDB
client=pymongo.MongoClient("mongodb+srv://balajiva33:balashupu@cluster0.csar8qe.mongodb.net/?retryWrites=true&w=majority") #mongodb://localhost:27017/"
db=client["Youtube_data"]

def channel_details(channel_id):
  ch_details=get_channel_info(channel_id)
  vi_ids=get_videos_ids(channel_id)
  vi_details=get_video_info(vi_ids)   #vi_details=get_video_info(video_ids) before replacing
  com_details=get_comment_info(vi_ids)  #com_details=get_comment_info(video_ids) before replacing

  coll1=db["channel_details"]
  coll1.insert_one({"channel_information":ch_details, "video_ids":vi_ids, "video_information":vi_details, "comment_information":com_details})

  return "upload completed successfully"

#table creation for channel, video & comment details
def channels_table():
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="balashupu",
                        database="youtube_data",
                        port="5432") #this the number used on postgre port
    cursor=mydb.cursor()

    drop_query='''drop table if exists channels'''
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query='''create table if not exists channels(Channel_Name varchar(100),
                                                        Channel_Id varchar(80) primary key,
                                                        Subscribers bigint,
                                                        Views bigint,
                                                        Total_Videos int,
                                                        Channel_Description text)'''
        cursor.execute(create_query)
        mydb.commit()

    except:
        print("channels table already created")

    ch_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0, "channel_information":1}): #empty {} implies to get all channel infos, if we want specifix id we have to fill the id in phrases
        ch_list.append(ch_data["channel_information"]) 
    df=pd.DataFrame(ch_list) #df is data frame which gives table format


    for index, row in df.iterrows(): #iterrows-data frame  into lists like views
        insert_query='''insert into channels(Channel_Name, 
                                            Channel_Id,
                                            Subscribers,
                                            Views,
                                            Total_Videos,
                                            Channel_Description)
                                            
                                            values(%s,%s,%s,%s,%s,%s)'''  #these terms are taken from table on POSTGRESQL
        values=(row["Channel_Name"],
                row["Channel_Id"],
                row["Subscribers"],
                row["Views"],
                row["Total_Videos"],
                row["Channel_Description"])  ##these terms are taken from dataframe table
        try: 
            cursor.execute(insert_query,values)
            mydb.commit()

        except:
            print("Channel values are already inserted")


def videos_table():

        mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="balashupu",
                        database="youtube_data",
                        port="5432") #this the number used on postgre port
        cursor=mydb.cursor()

        drop_query='''drop table if exists videos'''
        cursor.execute(drop_query)
        mydb.commit()
               

        create_query='''create table if not exists videos(Channel_Name varchar(100),
                                                        Channel_Id varchar(100),
                                                        Video_Id varchar(30) primary key,
                                                        Title varchar(150),
                                                        Tags text, 
                                                        Thumbnail varchar(200),
                                                        Description text,
                                                        Published_Date timestamp,
                                                        Duration interval,
                                                        views bigint,
                                                        Likes bigint,
                                                        Comments int,
                                                        Favorite_Count int,
                                                        Definition varchar(10),
                                                        Caption_status varchar(50)
                                                        )'''
        cursor.execute(create_query)
        mydb.commit()

        vi_list=[]
        db=client["Youtube_data"]
        coll1=db["channel_details"]
        for vi_data in coll1.find({},{"_id":0, "video_information":1}):
                for i in range(len(vi_data["video_information"])):
                        vi_list.append(vi_data["video_information"][i])
        df2=pd.DataFrame(vi_list)



        for index, row in df2.iterrows(): 
                insert_query='''insert into videos(Channel_Name,
                                                        Channel_Id,
                                                        Video_Id,
                                                        Title,
                                                        Tags, 
                                                        Thumbnail,
                                                        Description,
                                                        Published_Date,
                                                        Duration,
                                                        views,
                                                        Likes,
                                                        Comments,
                                                        Favorite_Count,
                                                        Definition,
                                                        Caption_status
                                                )
                                                
                                                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''' 
                values=(row["Channel_Name"],
                        row["Channel_Id"],
                        row["Video_Id"],
                        row["Title"],
                        row["Tags"],
                        row["Thumbnail"],
                        row["Description"],
                        row["Published_Date"],
                        row["Duration"],
                        row["views"],
                        row["Likes"],
                        row["Comments"],
                        row["Favorite_Count"],
                        row["Definition"],
                        row["Caption_status"],
                        ) 
        
        cursor.execute(insert_query,values)
        mydb.commit()

#To get comments table in SQL
def comments_table():
    
    mydb=psycopg2.connect(host="localhost",
                    user="postgres",
                    password="balashupu",
                    database="youtube_data",
                    port="5432") #this the number used on postgre port
    cursor=mydb.cursor()
    mydb.commit()

    drop_query = "drop table if exists comments"
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''CREATE TABLE if not exists comments(Comment_Id varchar(100) primary key,
                       Video_Id varchar(80),
                       Comment_Text text, 
                       Comment_Author varchar(150),
                       Comment_Published timestamp)'''
        cursor.execute(create_query)
        mydb.commit()
        
    except:
        st.write("Commentsp Table already created")

    com_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3 = pd.DataFrame(com_list)


    for index, row in df3.iterrows():
            insert_query = '''
                INSERT INTO comments (Comment_Id,
                                      Video_Id ,
                                      Comment_Text,
                                      Comment_Author,
                                      Comment_Published)
                VALUES (%s, %s, %s, %s, %s)

            '''
            values = (
                row['Comment_Id'],
                row['Video_Id'],
                row['Comment_Text'],
                row['Comment_Author'],
                row['Comment_Published']
            )
            try:
                cursor.execute(insert_query,values)
                mydb.commit()
            except:
               st.write("This comments are already exist in comments table")


def tables():
    channels_table()
    videos_table()
    comments_table()

    return "Tables Created Successfully"

def show_channels_table():
    ch_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0, "channel_information":1}): #empty {} implies to get all channel infos, if we want specifix id we have to fill the id in phrases
        ch_list.append(ch_data["channel_information"]) 
    df=st.dataframe(ch_list) 

    return df


def show_videos_table():
    vi_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for vi_data in coll1.find({},{"_id":0, "video_information":1}):
            for i in range(len(vi_data["video_information"])):
                    vi_list.append(vi_data["video_information"][i])
    df2=st.dataframe(vi_list)

    return df2

def show_comments_table():

        com_list=[]
        db=client["Youtube_data"]
        coll1=db["channel_details"]
        for com_data in coll1.find({},{"_id":0, "comment_information":1}):
                for i in range(len(com_data["comment_information"])):
                        com_list.append(com_data["comment_information"][i])
        df3=st.dataframe(com_list)

        return df3

#streamlit creation
with st.sidebar:
    st.title(":blue[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("Knowledge Development")
    st.caption("Python scripting")
    st.caption("Data collection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption("Data Management using MOngoDB & SQL")

channel_id=st.text_input("Enter the channel ID")

if st.button("Collect and store data"):
    ch_ids=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["Channel_Id"])

    if channel_id in ch_ids:
        st.success("channel details of the given channel Id exits")

    else:
        insert=channel_details(channel_id)
        st.success(insert)

if st.button("Migrate to sql"):
    Table=tables()
    st.success(Table)

show_table=st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS", "VIDEOS", "COMMENTS"))

if show_table=="CHANNELS": 
    show_channels_table()

elif show_table=="VIDEOS": 
    show_videos_table()

elif show_table=="COMMENTS": 
    show_comments_table()


#SQL Connection
mydb=psycopg2.connect(host="localhost",
                user="postgres",
                password="balashupu",
                database="youtube_data",
                port="5432") 
cursor=mydb.cursor()

question=st.selectbox("Select your question",("1. All the videos and the channel name",
                                            "2. Channels with most number of videos",
                                            "3. 10 most viewed videos",
                                            "4. Comments in each video",
                                            "5. Videos with highest likes",
                                            "6. Likes of all videos",
                                            "7. Views published in the year of 2022",
                                            "8. Videos published in the year of 2022",
                                            "9. Average duration of all videos in each channel",
                                            "10. videos with highest number of comments"))

if question=="1. All the videos and the channel name":
    query1='''select title as videos, channel_name as channelname from videos'''
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    df=pd.DataFrame(t1, columns=["video title", "channel name"])
    st.write(df)

elif question=="2. Channels with most number of videos":
    query2='''select channel_name as channelname, total_videos as no_videos from 
                channels order by total_videos desc'''
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()
    df2=pd.DataFrame(t2, columns=["channel name", "No of videos"])
    st.write(df2)

elif question=="3. 10 most viewed videos":
    query3='''select views as views, channel_name as channelname, title as videotitle from videos
                where views is not null order by views desc limit 10'''
    cursor.execute(query3)
    mydb.commit()
    t3=cursor.fetchall()
    df3=pd.DataFrame(t3, columns=["views", "channel name", "video title"])
    st.write(df3)

elif question=="4. Comments in each video":
    query4='''select comments as no_comments, title as videotitle from videos where comments is not null'''
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    df4=pd.DataFrame(t4, columns=["no of comments", "videotitle"])
    st.write(df4)

elif question=="5. Videos with highest likes":
    query5='''select title as videotitle, channel_name as channelname, likes as likecount
                from videos where likes is not null order by likes desc'''
    cursor.execute(query5)
    mydb.commit()
    t5=cursor.fetchall()
    df5=pd.DataFrame(t5, columns=["videotitle", "channelname", "likecount"])
    st.write(df5)

elif question=="6. Likes of all videos":  
    query6='''select title as likecount,title as videotitle from videos'''
    cursor.execute(query6)
    mydb.commit()
    t6=cursor.fetchall()
    df6=pd.DataFrame(t6, columns=["likecount", "videotitle"])
    st.write(df6)

elif question=="7. Views published in the year of 2022": 
    query7='''select channel_name as channelname, views as totalviews from channels'''
    cursor.execute(query7)
    mydb.commit()
    t7=cursor.fetchall()
    df7=pd.DataFrame(t7, columns=["channel", "videotitle"])
    st.write(df7)

elif question=="8. Videos published in the year of 2022": 
    query8='''select title as video_title, published_date as videorelease, channel_name as channelname from videos
                where extract(year from published_date)=2022'''
    cursor.execute(query8)
    mydb.commit()
    t8=cursor.fetchall()
    df8=pd.DataFrame(t8, columns=["videotitle", "published_date", "channelname"])
    st.write(df8)

elif question=="9. average duration of all videos in each channel": 
    query9='''select channel_name as channelname, AVG(duration) as averageduration from videos group by channel_name'''
    cursor.execute(query9)
    mydb.commit()
    t9=cursor.fetchall()
    df9=pd.DataFrame(t9, columns=["channelname", "averageduration"])
    

    T9=[] #to convert time duration, so to convert into str
    for index, row in df9.iterrows():
        channel_title=row["channelname"]
        average_duration=row["averageduration"]
        average_duration_str=str(average_duration)
        T9.append(dict(channeltitle=channel_title, avgduration=average_duration_str))
    df1=pd.DataFrame(T9)
    st.write(df1)

elif question=="10. Videos with highest number of comments": 
    query10='''select title as video_title, channel_name as channelname, comments as comments from videos where comments
                is not null order by comments desc'''
    cursor.execute(query10)
    mydb.commit()
    t10=cursor.fetchall()
    df10=pd.DataFrame(t10, columns=["videotitle", "channelname", "comments"])
    st.write(df10)
