from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st


api_service_name = "youtube"
api_version = "v3"

api_key = 'AIzaSyBHMWF0zzzyEUhtlx7Qakat7jD5R3U82yc'

youtube = build(
        api_service_name, api_version, developerKey=api_key)

def gets_channel(channel_id):
    request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id= channel_id
        )
    response = request.execute()
    
    for i in response['items']:
        data=dict(Channel_name=i['snippet']['title'],
                 Channel_id = i['id'],
                 Subscription_count = i['statistics']['subscriberCount'],
                 Total_Videos = i["statistics"]["videoCount"],
                 Channel_views = i['statistics']['viewCount'],
                 Channel_description = i['snippet']['description'],
                 Playlist_id = i['contentDetails']['relatedPlaylists']['uploads'])
        return data

#To get video id's

def gets_all_video_ids(channel_id):
    video_ids=[]
    
    response = youtube.channels().list(id=channel_id,
                                       part='contentDetails').execute()
    
    Playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token = None

    while True:
        response1 = youtube.playlistItems().list(
                                        part='snippet',
                                        maxResults=50,
                                        pageToken = next_page_token,
                                        playlistId=Playlist_id).execute()
        
        for i in range(len(response1['items'])):
                video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = response1.get('nextpageToken')
    
        if next_page_token is None:
            break
    return video_ids


# To get video informations

def get_all_video_datas(videos_ids):
    video_data = []
    for video_id in videos_ids:
        request = youtube.videos().list(
            part='snippet,contentDetails,statistics',
            id=video_id)
        response = request.execute()
    
        for item in response['items']:
            data=dict(Channel_name = item['snippet']['channelTitle'],
                      Video_Id = item['id'],
                      Video_Name = item['snippet']['title'],
                      Video_Description = item['snippet']['description'],
                      Tags = item['snippet'].get('tags'),
                      PublishedAt = item['snippet']['publishedAt'],
                      View_Count = item['statistics']['viewCount'],
                      Like_Count = item['statistics'].get('likeCount'),
                      Dislike_Count = item.get('dislikeCount'),
                      Favorite_Count = item['statistics']['favoriteCount'],
                      Comment_Count = item['statistics'].get('commentCount'),
                      Duration = item['contentDetails']['duration'],
                      Thumbnail = item['snippet']['thumbnails']['default']['url'],
                      Caption_Status = item['contentDetails']['caption']
                     )
            video_data.append(data)
    return video_data


#To get comment information

def get_all_comments_datas(videos_ids):
    Comments_data = []
    try:
        for vid_id in videos_ids:
            request = youtube.commentThreads().list(
                    part="snippet",
                    videoId = vid_id,
                    maxResults = 50
            )
            
            response = request.execute()
            
            for item in response['items']:
                data=dict(Comment_Id = item['snippet']['topLevelComment']['id'],
                          Comment_Text = item['snippet']['topLevelComment']['snippet']['textDisplay'],
                          Comment_Author = item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                          Comment_PublishedAt = item['snippet']['topLevelComment']['snippet']['publishedAt']
                         )
                Comments_data.append(data)
    
    except:
        pass
    return Comments_data


#MONGO DB

#Connected to my mongodb atlas by pasting the link and click connect.

client = pymongo.MongoClient("mongodb+srv://prashanth83362:2Ahpc0XEh65H1Ky8@cluster0.fj7k9kq.mongodb.net/?retryWrites=true&w=majority")
db=client["Youtube_data"]

def channel_details(channel_id):
    ch_details=gets_channel(channel_id)
    videos_ids=gets_all_video_ids(channel_id)
    vi_details=get_all_video_datas(videos_ids)
    cmts_details=get_all_comments_datas(videos_ids)

    coll1 = db["channel_details"]
    coll1.insert_one({"channel_info":ch_details,"video_info":vi_details,"comment_info":cmts_details})

    return "uploaded successfully"


#Creating channel tables in Postgres 
def channels_table():
    mydb=psycopg2.connect(host="localhost",
                          user="postgres",
                          password="shiva",
                          database="Youtube_data",
                          port="5432")
    
    cursor = mydb.cursor()
    
    drop_query = '''drop table if exists channels'''
    cursor.execute(drop_query)
    mydb.commit()
    
    try:
        create_query = '''create table if not exists channels(Channel_name varchar(255),
                                                              Channel_id varchar(255) primary key,
                                                              Subscription_count bigint,
                                                              Total_Videos int,
                                                              Channel_views bigint,
                                                              Channel_description text,
                                                              Playlist_id varchar(255))'''
    
        cursor.execute(create_query)
        mydb.commit()
    
    except:
        print("Done")
    
    
    ch_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_info":1}):
        ch_list.append(ch_data["channel_info"])
    df=pd.DataFrame(ch_list)
    
    
    for index,row in df.iterrows():
        insert_query = '''insert into channels(Channel_name,
                                               Channel_id,
                                               Subscription_count,
                                               Total_Videos,
                                               Channel_views,
                                               Channel_description,
                                               Playlist_id)
    
                                               values(%s,%s,%s,%s,%s,%s,%s)'''
        values = (row['Channel_name'],
                  row['Channel_id'],
                  row['Subscription_count'],
                  row['Total_Videos'],
                  row['Channel_views'],
                  row['Channel_description'],
                  row['Playlist_id'])
    
        try:
            cursor.execute(insert_query,values)
            mydb.commit()
    
        except:
            print("Error")


#Creating video tables in Postgres
def videos_table():
    mydb=psycopg2.connect(host="localhost",
                          user="postgres",
                          password="shiva",
                          database="Youtube_data",
                          port="5432")
    
    cursor = mydb.cursor()
    
    drop_query = '''drop table if exists videos'''
    cursor.execute(drop_query)
    mydb.commit()
    
    try:
        create_query = '''create table if not exists videos(Channel_name varchar(150),
                                                              Video_Id varchar(255) primary key,
                                                              Video_Name varchar(255),
                                                              Video_Description text,
                                                              Tags text,
                                                              PublishedAt timestamp,
                                                              View_Count int,
                                                              Like_Count int,
                                                              Dislike_Count int,
                                                              Favorite_Count int,
                                                              Comment_Count int,
                                                              Duration interval,
                                                              Thumbnail varchar(255),
                                                              Caption_Status varchar(255))'''
    
        cursor.execute(create_query)
        mydb.commit()
    
    except:
        print("Done")
    
    vid_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_info":1}):
        for i in range(len(vi_data["video_info"])):
            vid_list.append(vi_data["video_info"][i])
    df1=pd.DataFrame(vid_list)
    
    
    
    for index,row in df1.iterrows():
            insert_query = '''insert into videos(Channel_name,
                                                 Video_Id,
                                                 Video_Name,
                                                 Video_Description,
                                                 Tags,
                                                 PublishedAt,
                                                 View_Count,
                                                 Like_Count,
                                                 Dislike_Count,
                                                 Favorite_Count,
                                                 Comment_Count,
                                                 Duration,
                                                 Thumbnail,
                                                 Caption_Status
                                                )
        
                                                 values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
            values = (row['Channel_name'],
                      row['Video_Id'],
                      row['Video_Name'],
                      row['Video_Description'],
                      row['Tags'],
                      row['PublishedAt'],
                      row['View_Count'],
                      row['Like_Count'],
                      row['Dislike_Count'],
                      row['Favorite_Count'],
                      row['Comment_Count'],
                      row['Duration'],
                      row['Thumbnail'],
                      row['Caption_Status'])
    
        
        
            cursor.execute(insert_query,values)
            mydb.commit()

    


#Creating comment tables in Postgres
def comments_table():
    mydb=psycopg2.connect(host="localhost",
                              user="postgres",
                              password="shiva",
                              database="Youtube_data",
                              port="5432")
        
    cursor = mydb.cursor()
        
    drop_query = '''drop table if exists comments'''
    cursor.execute(drop_query)
    mydb.commit()
        
        
    create_query = '''create table if not exists comments(Comment_Id varchar(255) primary key,
                                                                  Comment_Text text,
                                                                  Comment_Author varchar(255),
                                                                  Comment_PublishedAt timestamp)'''
        
    cursor.execute(create_query)
    mydb.commit()
    
    com_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_info":1}):
        for i in range(len(com_data["comment_info"])):
            com_list.append(com_data["comment_info"][i])
    df2=pd.DataFrame(com_list)
    
    
    for index,row in df2.iterrows():
            insert_query = '''insert into comments(Comment_Id,
                                                   Comment_Text,
                                                   Comment_Author,
                                                   Comment_PublishedAt
                                                  )
                                                   values(%s,%s,%s,%s)'''
            values = (row['Comment_Id'],
                      row['Comment_Text'],
                      row['Comment_Author'],
                      row['Comment_PublishedAt'],
                      )
        
            
            try:
                cursor.execute(insert_query,values)
                mydb.commit()
            except:
                print("Error")


def tables():
    channels_table()
    videos_table()
    comments_table()

    return "Success ! - Tables created"


def show_channel_tables():
    ch_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_info":1}):
        ch_list.append(ch_data["channel_info"])
    df=st.dataframe(ch_list)
    return df


def show_video_tables():
    vid_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_info":1}):
        for i in range(len(vi_data["video_info"])):
            vid_list.append(vi_data["video_info"][i])
    df1=st.dataframe(vid_list)
    return df1


def show_comment_tables():
    com_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_info":1}):
        for i in range(len(com_data["comment_info"])):
            com_list.append(com_data["comment_info"][i])
    df2=st.dataframe(com_list)
    return df2


#Streamlit Part

with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("Skills takeaway from this project")
    st.caption("Python scripting")
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption("SQL")

channel_id = st.text_input("Enter the Channel ID")

if st.button("Collect and store data"):
    ch_ids=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_info":1}):
        ch_ids.append(ch_data["channel_info"]["Channel_id"])

    if channel_id in ch_ids:
        st.success("Channel ID already exists !")
    else:
        insert=channel_details(channel_id)
        st.success(insert)

if st.button("Migrate to SQL"):
    Table=tables()
    st.success(Table)

show_table = st.radio("SELECT THE TABLE TO VIEW",("CHANNELS","VIDEOS","COMMENTS"))

if show_table=="CHANNELS":
    show_channel_tables()

elif show_table=="VIDEOS":
    show_video_tables()

elif show_table=="COMMENTS":
    show_comment_tables()



#SQL CONNECTION

mydb=psycopg2.connect(host="localhost",
                              user="postgres",
                              password="shiva",
                              database="Youtube_data",
                              port="5432")
        
cursor = mydb.cursor()

question = st.selectbox("Select your question",("1.What are the names of all the videos and their corresponding channels?",
                                                "2.Which channels have the most number of videos, and how many videos do they have?",
                                                "3.What are the top 10 most viewed videos and their respective channels?",
                                                "4.How many comments were made on each video, and what are their corresponding video names?",
                                                "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
                                                "6.What is the total number of likes and dislikes for each video, and what are their corresponding videonames?",
                                                "7.What is the total number of views for each channel, and what are their corresponding channel names?",
                                                "8.What are the names of all the channels that have published videos in the year 2022?",
                                                "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                                "10.Which videos have the highest number of comments, and what are their corresponding channel names?"))

if question=="1.What are the names of all the videos and their corresponding channels?":
    query1 = '''select Video_Name as VideoNames, Channel_name as ChannelName from videos;'''
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    df=pd.DataFrame(t1,columns=[" Video Names","Channel Names"])
    st.write(df)

elif question=="2.Which channels have the most number of videos, and how many videos do they have?":
    query2 = '''select Channel_name as ChannelName,Total_Videos as Totalvideos from channels order by Total_Videos desc;'''
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()
    df1=pd.DataFrame(t2,columns=["Channel Names","Total Videos"])
    st.write(df1)

elif question=="3.What are the top 10 most viewed videos and their respective channels?":
    query3 = '''select View_Count as Views , Channel_name as ChannelName,Video_Name as VideoName from videos 
                where View_Count is not null order by View_Count desc limit 10;'''
    cursor.execute(query3)
    mydb.commit()
    t3=cursor.fetchall()
    df2=pd.DataFrame(t3,columns=["Views","Channel Name","Video Name"])
    st.write(df2)

elif question=="4.How many comments were made on each video, and what are their corresponding video names?":
    query4 = '''select Comment_Count as Comments ,Video_Name as VideoNames from videos where Comment_Count is not null;'''
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    df3=pd.DataFrame(t4,columns=["Comment Count","Video Name"])
    st.write(df3)

elif question=="5.Which videos have the highest number of likes, and what are their corresponding channel names?":
    query5 = '''select Video_Name as VideoNames, Channel_name as ChannelName, Like_Count as LikesCount from videos 
                    where Like_Count is not null order by Like_Count desc;'''
    cursor.execute(query5)
    mydb.commit()
    t5=cursor.fetchall()
    df4=pd.DataFrame(t5,columns=["Video Name","Channel Name","Likes Count"])
    st.write(df4)

elif question=="6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
    query6 = '''select Like_Count as likeCount,Dislike_Count as dislikeCount,Video_Name as VideoNames from videos;'''
    cursor.execute(query6)
    mydb.commit()
    t6=cursor.fetchall()
    df5=pd.DataFrame(t6,columns=["Likes Count","Dislikes Count","Video Name"])
    st.write(df5)

elif question=="7.What is the total number of views for each channel, and what are their corresponding channel names?":
    query7 = '''select Channel_name as ChannelName, Channel_views as Channelviews from channels;'''
    cursor.execute(query7)
    mydb.commit()
    t7=cursor.fetchall()
    df6=pd.DataFrame(t7,columns=["Channel Name","Channel Views"])
    st.write(df6) 

elif question=="8.What are the names of all the channels that have published videos in the year 2022?":
    query8 = '''select Video_Name as VideoNames, PublishedAt as VideoRelease, Channel_name as ChannelName from videos 
                    where extract(year from PublishedAt) = 2022;'''
    cursor.execute(query8)
    mydb.commit()
    t8=cursor.fetchall()
    df7=pd.DataFrame(t8,columns=["Video Name","Published At","Channel Name"])
    st.write(df7)

elif question=="9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    query9 = '''SELECT Channel_Name as ChannelName, AVG(Duration) AS average_duration FROM videos GROUP BY Channel_Name;'''
    cursor.execute(query9)
    mydb.commit()
    t9=cursor.fetchall()
    df8=pd.DataFrame(t9,columns=["Channel Name","Average Duration"])
    
    T9=[]
    for index,row in df8.iterrows():
        channel_title = row["Channel Name"]
        average_duration = row["Average Duration"]
        average_duration_str = str(average_duration)
        T9.append(dict(channeltitle = channel_title,avgduration = average_duration_str))
    df1 = pd.DataFrame(T9)
    st.write(df1)

elif question=="10.Which videos have the highest number of comments, and what are their corresponding channel names?":
    query10 = '''select Video_Name as Video_names,Channel_name as Channelname,Comment_Count as Comments from videos 
                    where Comment_Count is not null order by Comment_Count desc;'''
    cursor.execute(query10)
    mydb.commit()
    t10=cursor.fetchall()
    df9=pd.DataFrame(t10,columns=["Video Name","Channel Name","Comments Count"])
    st.write(df9)
