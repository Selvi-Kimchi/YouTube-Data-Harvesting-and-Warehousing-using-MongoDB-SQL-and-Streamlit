'''Importing Libraries
1. googleapiclient: This library is used to interact with Google APIs, such as the Google Sheets API, which allows reading and writing data to Google Sheets.
2. psycopg2: This library is for interacting with PostgreSQL databases using Python.
3. pandas: Pandas is a powerful data manipulation and analysis library in Python, often used for handling tabular data.
4. streamlit: Streamlit is a Python library used for creating web applications for machine learning and data science projects. It allows for creating interactive web interfaces directly from Python scripts.
5. pymongo: This library is used for interacting with MongoDB, a popular NoSQL database, from Python.
'''
from googleapiclient.discovery import build
import psycopg2
import pandas as pd 
import streamlit as st
from googleapiclient.errors import HttpError
from pymongo import MongoClient


# Connection to the YouTube Data API using API key
def api_connect():
    API_key ='AIzaSyDIK7n4NFw1bH_JR42IKlydomA0*******'
    api_service_name='youtube'
    api_version='v3'
    youtube=build(api_service_name,api_version, developerKey=API_key)
    return youtube
youtube=api_connect()


#Retrieving information about a YouTube channel using its channel_id
def get_channel_info(channel_id):
    request = youtube.channels().list(
        part="snippet, ContentDetails, statistics", 
        id=channel_id   
    )
    response=request.execute()
    for i in response['items']:
        data=dict(Channel_Name=i['snippet']['title'],
                Channel_Id=i['id'],
                Subscribers_Count=i['statistics']['subscriberCount'],
                View_Count=i['statistics']['viewCount'],
                Total_Videos=i['statistics']['videoCount'],
                Channel_description=i['snippet']['description'],
                Playlist_Id=i['contentDetails']['relatedPlaylists']['uploads']
                )
        return data
    

#Retrieving the video IDs of a YouTube channel
def get_video_ids(channel_id):
    video_ids=[]
    request = youtube.channels().list(part="ContentDetails", id=channel_id)
    response=request.execute()
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token=None
    while True:
        response1=youtube.playlistItems().list(
            part='snippet', 
            playlistId=Playlist_Id,
            maxResults=50,
            pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids


#Retrieving video information for a list of YouTube video IDs
def get_video_info(video_ids):
    video_data=[]
    for video_id in video_ids:
        request=youtube.videos().list(
            part="snippet, ContentDetails, statistics", 
            id = video_id
        )
        response=request.execute()
        for i in response['items']:
            data=dict(Channel_Name=i['snippet']['channelTitle'],
                Channel_Id=i['snippet']['channelId'],
                Video_Id=i['id'],
                Title=i['snippet']['title'],
                Tags=i['snippet'].get('tags'),
                Thumbnail=i['snippet']['thumbnails']['default']['url'],
                Description=i['snippet'].get('description'),
                Published_Date=i['snippet']['publishedAt'],
                Duration=i['contentDetails']['duration'],
                Views=i['statistics'].get('viewCount'),
                Likes=i['statistics'].get('likeCount'),
                Dislikes=i['statistics'].get('dislikeCount'),
                Comments=i['statistics'].get('commentCount'),
                Favorite_Count=i['statistics']['favoriteCount'],
                Definition=i['contentDetails']['definition'],
                Caption_Status=i['contentDetails']['caption']
                )
        video_data.append(data) 
    return video_data


#Retrieving playlist details using channel ID.
def get_playlist_details(channel_id):
    next_page_token=None
    playlist_data=[]
    while True:
        request=youtube.playlists().list(
            part="snippet, ContentDetails", 
            channelId = channel_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response=request.execute()
        for i in response['items']:
            data=dict(Playlist_Id=i['id'],
                Title=i['snippet']['title'],
                Channel_Id=i['snippet']['channelId'],
                Channel_Name=i['snippet']['channelTitle'],
                PublishedAt=i['snippet']['publishedAt'],
                Video_Count=i['contentDetails']['itemCount']
                )
            playlist_data.append(data) 
        next_page_token=response.get('nextPageToken')
        if next_page_token is None:
            break
    return playlist_data


#Retrieving comment information for a list of YouTube video IDs
def get_comment_info(video_ids):
    comments = []

    for video_id in video_ids:
        try:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id
            )
            response = request.execute()

            for item in response.get('items', []):
                comment_data = {
                    'Comment_Id': item['snippet']['topLevelComment']['id'],
                    'Video_Id':item['snippet']['topLevelComment']['snippet']['videoId'],
                    'Comment_Text': item['snippet']['topLevelComment']['snippet']['textDisplay'],
                    'Comment_Author': item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    'Comment_PublishedAt': item['snippet']['topLevelComment']['snippet']['publishedAt']
                }
                comments.append(comment_data)

        except HttpError as e:
            error_details = e.content.decode('utf-8')
            if 'commentsDisabled' in error_details:
                print(f"Comments are disabled for video {video_id}. Skipping.")
            else:
                print(f"Error retrieving comments for video {video_id}: {e}")

    return comments


# Connection to MongoDB
mongo_client = MongoClient('mongodb+srv://SelviK:*******@cluster0.aqotcci.mongodb.net/?retryWrites=true&w=majority')
mongo_db = mongo_client['YouTubeDatabase']
collection1=mongo_db["YoutubeChannel_Details"]


# Uploading Channel Details to MongoDB
def channel_details(channel_id):
    channel_details=get_channel_info(channel_id)
    playlist_details=get_playlist_details(channel_id)
    video_ids=get_video_ids(channel_id)
    video_details=get_video_info(video_ids)
    comment_details=get_comment_info(video_ids)
    collection1=mongo_db["YoutubeChannel_Details"]
    collection1.insert_one({"Channel_Information": channel_details,
                            "Playlist_Information": playlist_details,
                            "Video_Information": video_details,
                            "Comment_Information": comment_details})
    return "uploaded successfully"


# Connection to PostgreSQL Database
mydb=psycopg2.connect(host="localhost",
                      user="postgres",
                      password="*******",
                      database="YouTubeDataNew",
                      port="5432")
cursor=mydb.cursor()


# Creating the Channels Table in SQL
def channels_table_create():
    mydb=psycopg2.connect(host="localhost",
                      user="postgres",
                      password="*******",
                      database="YouTubeDataNew",
                      port="5432")
    cursor=mydb.cursor()
    drop_query='''drop table if exists channels'''
    cursor.execute(drop_query)
    mydb.commit()
    try:
        create_query='''create table if not exists channels(Channel_Name varchar(100),
                                                            Channel_Id varchar(50) primary key,
                                                            Subscribers_Count bigint,
                                                            View_Count bigint,
                                                            Total_Videos int,
                                                            Channel_description text,
                                                            Playlist_Id varchar(80))'''
        cursor.execute(create_query)
        mydb.commit()

    except:
        print("Channel table already created")
        
    mongo_db = mongo_client['YouTubeDatabase']
    collection1=mongo_db["YoutubeChannel_Details"]
    channel_list=[]
    for channel_data in collection1.find({}, {"_id":0,"Channel_Information":1}):
        channel_list.append(channel_data["Channel_Information"])
    df=pd.DataFrame(channel_list)

    for index, row in df.iterrows():
        insert_query='''insert into channels(Channel_Name,
                                            Channel_Id,
                                            Subscribers_Count,
                                            View_Count,
                                            Total_Videos,
                                            Channel_description,
                                            Playlist_Id)
                                            values(%s, %s,%s,%s,%s,%s,%s)'''
        values=(row['Channel_Name'],
                row['Channel_Id'],
                row['Subscribers_Count'],
                row['View_Count'],
                row['Total_Videos'],
                row['Channel_description'],
                row['Playlist_Id'])
        
        try:
            cursor.execute(insert_query, values)
            mydb.commit()

        except:
            print("Channels values are already inserted")

            
# Creating the Playlist Table in SQL
def playlist_table_create():
        mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="*******",
                        database="YouTubeDataNew",
                        port="5432")
        cursor=mydb.cursor()
        drop_query='''drop table if exists playlists'''
        cursor.execute(drop_query)
        mydb.commit()
        create_query='''create table if not exists playlists(Playlist_Id varchar(100) primary key,
                                                        Title varchar(100),
                                                        Channel_Id varchar(80),
                                                        Channel_Name varchar(80),
                                                        PublishedAt timestamp,
                                                        Video_Count int)'''
        
        cursor.execute(create_query)
        mydb.commit()

        mongo_db = mongo_client['YouTubeDatabase']
        collection1=mongo_db["YoutubeChannel_Details"]

        playlist_list=[]
        for playlist_data in collection1.find({}, {"_id":0,"Playlist_Information":1}):
                for i in range(len(playlist_data["Playlist_Information"])):
                        playlist_list.append(playlist_data["Playlist_Information"][i])
        df=pd.DataFrame(playlist_list)
        
        for index, row in df.iterrows():
                insert_query='''insert into playlists(Playlist_Id,
                                                        Title,
                                                        Channel_Id,
                                                        Channel_Name,
                                                        PublishedAt,
                                                        Video_Count)
                                                        values(%s, %s,%s,%s,%s,%s)'''
                values=(row['Playlist_Id'],
                        row['Title'],
                        row['Channel_Id'],
                        row['Channel_Name'],
                        row['PublishedAt'],
                        row['Video_Count'])
                        
                
                cursor.execute(insert_query, values)
                mydb.commit()


# Creating the Videos Table in SQL
def video_table_create():
        mydb=psycopg2.connect(host="localhost",
                user="postgres",
                password="*******",
                database="YouTubeDataNew",
                port="5432")
        cursor=mydb.cursor()
        drop_query='''drop table if exists videos'''
        cursor.execute(drop_query)
        mydb.commit()
        create_query='''create table if not exists videos(Channel_Name varchar(100),
                                                        Channel_Id varchar(80),
                                                        Video_Id varchar(40) primary key,
                                                        Title varchar(100),
                                                        Tags text,
                                                        Thumbnail varchar(200),
                                                        Description text,                                            
                                                        Published_Date timestamp,
                                                        Duration interval,
                                                        Views bigint,
                                                        Likes bigint,
                                                        Dislikes int,
                                                        Comments int,
                                                        Favorite_Count int,
                                                        Definition varchar(10),
                                                        Caption_Status varchar(10)
                                                        )'''

        cursor.execute(create_query)
        mydb.commit()
        mongo_db = mongo_client['YouTubeDatabase']
        collection1=mongo_db["YoutubeChannel_Details"]

        video_list=[]
        for video_data in collection1.find({}, {"_id":0,"Video_Information":1}):
                for i in range(len(video_data["Video_Information"])):
                        video_list.append(video_data["Video_Information"][i])
        df=pd.DataFrame(video_list)

        for index, row in df.iterrows():
                insert_query='''insert into videos(Channel_Name,
                                                        Channel_Id,
                                                        Video_Id,
                                                        Title,
                                                        Tags,
                                                        Thumbnail,
                                                        Description,                                            
                                                        Published_Date,
                                                        Duration,
                                                        Views,
                                                        Likes,
                                                        Dislikes,
                                                        Comments,
                                                        Favorite_Count,
                                                        Definition,
                                                        Caption_Status)
                                                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
                values=(row['Channel_Name'],
                        row['Channel_Id'],
                        row['Video_Id'],
                        row['Title'],
                        row['Tags'],
                        row['Thumbnail'],
                        row['Description'],
                        row['Published_Date'],
                        row['Duration'],
                        row['Views'],
                        row['Likes'],
                        row['Dislikes'],
                        row['Comments'],
                        row['Favorite_Count'],
                        row['Definition'],
                        row['Caption_Status']        
                        )
                        

                cursor.execute(insert_query, values)
                mydb.commit()


# Creating the Comments Table in SQL                
def comment_table_create():
    mydb=psycopg2.connect(host="localhost",
                user="postgres",
                password="*******",
                database="YouTubeDataNew",
                port="5432")
    cursor=mydb.cursor()
    drop_query='''drop table if exists comments'''
    cursor.execute(drop_query)
    mydb.commit()
    create_query='''create table if not exists comments(Comment_Id varchar(100) primary key,
                                                    Video_Id varchar(80),
                                                    Comment_Text text,
                                                    Comment_Author varchar(50),
                                                    Comment_PublishedAt timestamp)'''

    cursor.execute(create_query)
    mydb.commit()
    mongo_db = mongo_client['YouTubeDatabase']
    collection1=mongo_db["YoutubeChannel_Details"]

    comment_list=[]
    for comment_data in collection1.find({}, {"_id":0,"Comment_Information":1}):
            for i in range(len(comment_data["Comment_Information"])):
                    comment_list.append(comment_data["Comment_Information"][i])
    df=pd.DataFrame(comment_list)
    for index, row in df.iterrows():
        insert_query='''insert into comments(Comment_Id,
                                                Video_Id,
                                                Comment_Text,
                                                Comment_Author,
                                                Comment_PublishedAt)
                                                values(%s,%s,%s,%s,%s)'''
        values=(row['Comment_Id'],
                row['Video_Id'],
                row['Comment_Text'],
                row['Comment_Author'],
                row['Comment_PublishedAt']
                )
                
        
        cursor.execute(insert_query, values)
        mydb.commit()


# Creating and Updating Required Tables in SQL
def tables():
    channels_table_create()
    playlist_table_create()
    video_table_create()
    comment_table_create()
    return "Required Tables are created and updated successfully"


# Streamlit Integration
# Defining the Content of the Streamlit User Interface Sidebar
with st.sidebar:
    st.markdown("## Key Learning Objectives")
    st.write("- **Data Collection using YouTube API**")
    st.write("- **Effective Data Management using MongoDB and SQL Databases**")
    st.write("- **Streamlit for Interactive Data Visualization**")

    
    st.subheader("Resources:")
    st.markdown("- [YouTube API Documentation](https://developers.google.com/youtube/v3/docs/)")
    st.markdown("- [Streamlit Documentation](https://docs.streamlit.io)")
    st.markdown("- [MongoDB Documentation](https://www.mongodb.com/docs/)")
    st.markdown("- [SQL Documentation](https://www.postgresql.org/docs/)")
    
st.markdown("## YouTube Data Harvesting and Warehousing")   


## Main Content:
channel_id=st.text_input("**Enter the channel ID**:") 
if st.button(":red[Fetch and Transfer Data]"):
    mongo_db = mongo_client['YouTubeDatabase']
    collection1=mongo_db["YoutubeChannel_Details"]
    channel_ids=[]
    for channel_data in collection1.find({}, {"_id":0,"Channel_Information":1}):
        channel_ids.append(channel_data["Channel_Information"]["Channel_Id"])
    
    if channel_id in channel_ids:
        st.success("This channel details already exists")
    else:
        insert=channel_details(channel_id)
        st.success(insert)
        table=tables()
        st.success(table)

def show_channels_table():
    mongo_db = mongo_client['YouTubeDatabase']
    collection1=mongo_db["YoutubeChannel_Details"]
    channel_list=[]
    for channel_data in collection1.find({}, {"_id":0,"Channel_Information":1}):
        channel_list.append(channel_data["Channel_Information"])
    df=st.dataframe(channel_list)
    return df

def show_playlists_table():
    mongo_db = mongo_client['YouTubeDatabase']
    collection1=mongo_db["YoutubeChannel_Details"]
    playlist_list=[]
    for playlist_data in collection1.find({}, {"_id":0,"Playlist_Information":1}):
            for i in range(len(playlist_data["Playlist_Information"])):
                    playlist_list.append(playlist_data["Playlist_Information"][i])
    df=st.dataframe(playlist_list)
    return df

def show_videos_table():
    mongo_db = mongo_client['YouTubeDatabase']
    collection1=mongo_db["YoutubeChannel_Details"]
    video_list=[]
    for video_data in collection1.find({}, {"_id":0,"Video_Information":1}):
            for i in range(len(video_data["Video_Information"])):
                    video_list.append(video_data["Video_Information"][i])
    df=st.dataframe(video_list)
    return df

def show_comments_table():
    mongo_db = mongo_client['YouTubeDatabase']
    collection1=mongo_db["YoutubeChannel_Details"]
    comment_list=[]
    for comment_data in collection1.find({}, {"_id":0,"Comment_Information":1}):
            for i in range(len(comment_data["Comment_Information"])):
                    comment_list.append(comment_data["Comment_Information"][i])
    df=st.dataframe(comment_list)
    return df

show_table=st.radio("**SELECT THE TABLE FOR VIEW**", ("Channels", "Playlists", "Videos", "Comments"))

if show_table=="Channels":
    st.subheader("Channel Details")
    show_channels_table()

elif show_table=="Playlists":
    st.subheader("Playlists Details")
    show_playlists_table()

elif show_table=="Videos":
    st.subheader("Videos Details")
    show_videos_table()
else:
    st.subheader("Comments Details")
    show_comments_table()


#Question and answer session
mydb=psycopg2.connect(host="localhost",
                user="postgres",
                password="*******",
                database="YouTubeDataNew",
                port="5432")
cursor=mydb.cursor()


question = st.selectbox("Select your question", (
    "1. What are the names of all the videos and their corresponding channels?",
    "2. Which channels have the most number of videos, and how many videos do they have?",
    "3. What are the top 10 most viewed videos and their respective channels?",
    "4. How many comments were made on each video, and what are their corresponding video names?",
    "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
    "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
    "7. What is the total number of views for each channel, and what are their corresponding channel names?",
    "8. What are the names of all the channels that have published videos in the year 2022?",
    "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
    "10. Which videos have the highest number of comments, and what are their corresponding channel names?"
))

if question=="1. What are the names of all the videos and their corresponding channels?":
    query1='''select title as videos, Channel_Name as channelname from videos'''
    cursor.execute(query1)
    mydb.commit()
    q1=cursor.fetchall()
    df1=pd.DataFrame(q1, columns=["video_title", "channel_name"])
    st.write(df1)

elif question=="2. Which channels have the most number of videos, and how many videos do they have?":
    query2='''select channel_name as channelname, total_videos as tol_videos from channels 
                order by total_videos desc'''
    cursor.execute(query2)
    mydb.commit()
    q2=cursor.fetchall()
    df2=pd.DataFrame(q2, columns=["channel_name", "No of videos"])
    st.write(df2)

elif question=="3. What are the top 10 most viewed videos and their respective channels?":
    query3='''select views as views, channel_name as channelname, title as videotitle from videos
                where views is not null order by views desc limit 10'''
    cursor.execute(query3)
    mydb.commit()
    q3=cursor.fetchall()
    df3=pd.DataFrame(q3, columns=["views_count", "channel name", "video_title"])
    st.write(df3)

elif question=="4. How many comments were made on each video, and what are their corresponding video names?":
    query4='''select comments as comments, title as videoname from videos where comments is not null'''
    cursor.execute(query4)
    mydb.commit()
    q4=cursor.fetchall()
    df4=pd.DataFrame(q4, columns=["comments_count", "video_title"])
    st.write(df4)

elif question=="5. Which videos have the highest number of likes, and what are their corresponding channel names?":
    query5='''select  title as videotitle, channel_name as channelname, likes as likes from videos where likes is not null order by likes desc'''
    cursor.execute(query5)
    mydb.commit()
    q5=cursor.fetchall()
    df5=pd.DataFrame(q5, columns=["video_title", "channel_name","likes_count"])
    st.write(df5)

elif question=="6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
    query6='''select  likes as likes, dislikes as dislikes, title as videotitle from videos where likes is not null'''
    cursor.execute(query6)
    mydb.commit()
    q6=cursor.fetchall()
    df6=pd.DataFrame(q6, columns= ["likes_count", "dislikes_count","video_title",])
    st.write(df6)
   
   
elif question=="7. What is the total number of views for each channel, and what are their corresponding channel names?":
    query7='''select  channel_name as channelname, view_count as total_views from channels where view_count is not null'''
    cursor.execute(query7)
    mydb.commit()
    q7=cursor.fetchall()
    df7=pd.DataFrame(q7, columns=["channel_name","views_count"])
    st.write(df7)
   

elif question=="8. What are the names of all the channels that have published videos in the year 2022?":
    query8='''select title as video_title, published_date as videoreleasedate, channel_name as channelname from videos
            where extract(year from published_date)=2022'''
    cursor.execute(query8)
    mydb.commit()
    q8=cursor.fetchall()
    df8=pd.DataFrame(q8, columns=["video_title", "published_date", "channel_name"])
    st.write(df8)
   
elif question=="9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    query9='''select channel_name as channelname,  AVG(duration) as averageduration from videos group by channel_name'''
    cursor.execute(query9)
    mydb.commit()
    q9=cursor.fetchall()
    df9=pd.DataFrame(q9, columns=["channelname","averageduration"])
    formatted_durations = []
    for index, row in df9.iterrows():
        channel_title = row["channelname"] 
        duration_seconds = row["averageduration"].total_seconds()
        hours = int(duration_seconds // 3600)
        minutes = int((duration_seconds % 3600) // 60)
        seconds = int(duration_seconds % 60)
        formatted_duration = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        formatted_durations.append(dict(channeltitle=channel_title, avgduration=formatted_duration))
    df_9 = pd.DataFrame(formatted_durations)
    st.write(df_9)
   
else: 
    #question=="10. Which videos have the highest number of comments, and what are their corresponding channel names?":
    query10='''select  title as videotitle, channel_name as channelname, comments as comments from videos 
    where comments is not null order by comments desc'''
    cursor.execute(query10)
    mydb.commit()
    q10=cursor.fetchall()
    df10=pd.DataFrame(q10, columns=["video_title", "channel_name","comments_count"])
    st.write(df10)
   



