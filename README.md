# YouTube-Data-Harvesting-and-Warehousing

Problem Statement:

The problem statement is to create a Streamlit application that allows users to access and analyze data from multiple YouTube channels. The application should have the following features:
1.  Ability to input a YouTube channel ID and retrieve all the relevant data (Channel name, subscribers, total video count, playlist ID, video ID, likes, dislikes, comments of each video) using Google API.
2. Option to store the data in a MongoDB database as a data lake.
3. Ability to collect data for up to 10 different YouTube channels and store them in the data lake by clicking a button.
4. Option to select a channel name and migrate its data from the data lake to a SQL database as tables.
5.Ability to search and retrieve data from the SQL database using different search options, including joining tables to get channel details.


TOOLS AND LIBRARIES USED:

STREAMLIT: Streamlit library was used to create a user-friendly UI that enables users to interact with the programme and carry out data retrieval and analysis operations.

PYTHON: Python is a powerful programming language renowned for being easy to learn and understand. Python is the primary language employed in this project for the development of the complete application, including data retrieval, processing, analysis, and visualisation.

GOOGLE API CLIENT: The googleapiclient library in Python facilitates the communication with different Google APIs. Its primary purpose in this project is to interact with YouTube's Data API v3, allowing the retrieval of essential information like channel details, video specifics, and comments. By utilizing googleapiclient, developers can easily access and manipulate YouTube's extensive data resources through code.

MONGODB: MongoDB is built on a scale-out architecture that has become popular with developers of all kinds for developing scalable applications with evolving data schemas. As a document database, MongoDB makes it easy for developers to store structured or unstructured data. It uses a JSON-like format to store documents.

POSTGRESQL: PostgreSQL is an open-source, advanced, and highly scalable database management system (DBMS) known for its reliability and extensive features. It provides a platform for storing and managing structured data, offering support for various data types and advanced SQL capabilities.

REQUIRED LIBRARIES:

1.googleapiclient.discovery

2.streamlit

3.psycopg2

4.pymongo

5.pandas

FEATURES: 
The following functions are available in the YouTube Data Harvesting and Warehousing application: 

1. Retrieval of channel and video data from YouTube using the YouTube API.
2. Storage of data in a MongoDB database as a data lake.
3. Migration of data from the data lake to a SQL database for efficient querying and analysis.
4. Search and retrieval of data from the SQL database using different search options



