from pyspark.sql import SQLContext, Row, DataFrame, SparkSession
from pyspark.sql.types import *
import os
import pandas
from sqlalchemy import create_engine
from pyspark.sql import DataFrameReader

def load_badges(ds, **kwargs):
    tbl_name='badges_landing'
    file_name='outfittery_challenge\Badges.xml'
    customSchema = StructType([ \
        StructField("_UserId", LongType(), True), \
        StructField("_Name", StringType(), True), \
        StructField("_Date", StringType(), True)]
    )
    extract_load(tbl_name,file_name,customSchema)

def load_comments(ds, **kwargs):
    tbl_name='comments_landing'
    file_name='outfittery_challenge\Comments.xml'
    customSchema = StructType([ \
        StructField("_Id", LongType(), True), \
        StructField("_PostId", LongType(), True), \
        StructField("_Score", LongType(), True), \
        StructField("_Text", StringType(), True), \
        StructField("_CreationDate", StringType(), True), \
        StructField("_UserId", LongType(), True)]
    )
    extract_load(tbl_name,file_name,customSchema)

def load_posts(ds, **kwargs):
    tbl_name='posts_landing'
    file_name='outfittery_challenge\Posts.xml'
    customSchema = StructType([ \
        StructField("_Id", LongType(), True), \
        StructField("_PostTypeId", LongType(), True), \
        StructField("_ParentId", LongType(), True), \
        StructField("_AcceptedAnswerId", LongType(), True), \
        StructField("_CreationDate", StringType(), True), \
        StructField("_Score", LongType(), True), \
        StructField("_ViewCount", LongType(), True), \
        StructField("_Body", StringType(), True), \
        StructField("_OwnerUserId", LongType(), True),
        StructField("_LastEditorUserId", LongType(), True), \
        StructField("_LastEditorDisplayName", StringType(), True), \
        StructField("_LastEditDate", StringType(), True), \
        StructField("_LastActivityDate", StringType(), True), \
        StructField("_CommunityOwnedDate", StringType(), True), \
        StructField("_ClosedDate", StringType(), True), \
        StructField("_Title", StringType(), True), \
        StructField("_Tags", StringType(), True), \
        StructField("_AnswerCount", LongType(), True), \
        StructField("_CommentCount", LongType(), True), \
        StructField("_FavoriteCount", LongType(), True)]
    )
    extract_load(tbl_name,file_name,customSchema)

def load_postlinks(ds, **kwargs):
    tbl_name='postlinks_landing'
    file_name='outfittery_challenge\PostLinks.xml'
    customSchema = StructType([ \
        StructField("_Id", LongType(), True), \
        StructField("_CreationDate", StringType(), True), \
        StructField("_PostId", LongType(), True), \
        StructField("_RelatedPostId", LongType(), True), \
        StructField("_PostLinkTypeId", LongType(), True), \
        StructField("_UserId", LongType(), True)]
    )
    extract_load(tbl_name,file_name,customSchema)

def load_users(ds, **kwargs):
    tbl_name='users_landing'
    file_name='outfittery_challenge\\Users.xml'
    customSchema = StructType([ \
        StructField("_Id", LongType(), True), \
        StructField("_Reputation", LongType(), True), \
        StructField("_CreationDate", StringType(), True), \
        StructField("_DisplayName", StringType(), True), \
        StructField("_EmailHash", StringType(), True), \
        StructField("_LastAccessDate", StringType(), True), \
        StructField("_WebsiteUrl", StringType(), True), \
        StructField("_Location", StringType(), True), \
        StructField("_Age", LongType(), True), \
        StructField("_AboutMe", StringType(), True), \
        StructField("_Views", LongType(), True), \
        StructField("_UpVotes", LongType(), True), \
        StructField("_DownVotes", LongType(), True), \
        StructField("_ProfileImageUrl", StringType(), True),
        StructField("_AccountId", LongType(), True)]
    )
    extract_load(tbl_name,file_name,customSchema)

def load_votes(ds, **kwargs):
    tbl_name='votes_landing'
    file_name='outfittery_challenge\Votes.xml'
    customSchema = StructType([ \
        StructField("_Id", LongType(), True), \
        StructField("_PostId", LongType(), True), \
        StructField("_VoteTypeId", LongType(), True), \
        StructField("_CreationDate", StringType(), True), \
        StructField("_UserId", LongType(), True), \
        StructField("_BountyAmount", LongType(), True)]
    )
    extract_load(tbl_name,file_name,customSchema)

def load_posthistory(ds, **kwargs):
    tbl_name='posthistory_landing'
    file_name='outfittery_challenge\PostHistory.xml'
    customSchema = StructType([ \
        StructField("_Id", LongType(), True), \
        StructField("_PostHistoryTypeId", LongType(), True), \
        StructField("_PostId", LongType(), True), \
        StructField("_RevisionGUID", StringType(), True), \
        StructField("_CreationDate", StringType(), True), \
        StructField("_UserId", LongType(), True), \
        StructField("_UserDisplayName", StringType(), True), \
        StructField("_Comment", StringType(), True), \
        StructField("_Text", StringType(), True), \
        StructField("_CloseReasonId", LongType(), True)]
    )
    extract_load(tbl_name,file_name,customSchema)

def extract_load(tbl_name,file_name,customSchema):
    number_of_rows = 30 # Limiting 30 rows for testing purposes
    connection_url = 'postgres://ezaqmkuy:QAFiKQa4vlSO2jZMSsaI65n9K7k0YZLc@rajje.db.elephantsql.com:5432/ezaqmkuy' # Better to use PostgresSQlHook provided by Airflow
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.databricks:spark-xml_2.11:0.8.0 --jars postgresql-42.2.9.jar pyspark-shell'
    spark = SparkSession.builder.getOrCreate()
    df = spark.read \
        .format('xml') \
        .options(rowTag='row') \
        .load(file_name, schema = customSchema).limit(number_of_rows)
    engine = create_engine(connection_url)
    df.toPandas().to_sql(tbl_name,engine,if_exists='replace')

# if __name__ == '__main__':
#     load_users('')
