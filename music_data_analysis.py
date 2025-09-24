# Part 1: Setup and Data Loading

from google.colab import drive
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, row_number, split, explode, collect_list, sum as spark_sum, asc
from pyspark.sql.window import Window
import matplotlib.pyplot as plt
import pandas as pd

drive.mount('/content/drive')
spark = SparkSession.builder.appName("MusicAnalysis").getOrCreate()
base_path = '/content/drive/MyDrive/dataset/'

listening_df = spark.read.csv(base_path + 'listenings.csv', header=True, inferSchema=True)
genre_df = spark.read.csv(base_path + 'genre.csv', header=True, inferSchema=True)

# Part 2: Data Cleaning and Schema

listening_df = listening_df.dropna(subset=['user_id', 'track', 'artist'])
genre_df = genre_df.dropna(subset=['artist', 'genre'])

print("Initial data frames loaded and cleaned.")
print("Listening data schema:")
listening_df.printSchema()

# Part 3: Initial Investigation of the dataset

# 1. All records for users who listened to Rihanna
rihanna_records = listening_df.filter(col("artist") == "rihanna")
rihanna_records.show(5)

# 2. Top 10 fans of Rihanna
rihanna_fans = rihanna_records.groupBy("user_id").count().sort(desc("count"))
rihanna_fans.show(10)

# 3. Top 10 famous tracks
top_tracks = listening_df.groupBy("track").count().sort(desc("count"))
top_tracks.show(10)

# 4. Top 10 famous tracks of Rihanna
top_rihanna_tracks = rihanna_records.groupBy("track").count().sort(desc("count"))
top_rihanna_tracks.show(10)

# 5. Top 10 famous albums
top_albums = listening_df.groupBy("album").count().sort(desc("count"))
top_albums.show(10)

# Part 4: Advanced Analysis of the dataset

joined_df = listening_df.join(genre_df, on="artist", how="inner")

# 6. Top 10 users who are fans of pop music
pop_music_fans = joined_df.filter(col("genre") == "pop").groupBy("user_id").count().sort(desc("count"))
pop_music_fans.show(10)

# 7. Top 10 famous genres
top_genres = joined_df.groupBy("genre").count().sort(desc("count"))
top_genres.show(10)

# 8. Each user's favorite genre (using Window Functions)
window_spec = Window.partitionBy("user_id").orderBy(desc("count"))
user_genre_counts = joined_df.groupBy("user_id", "genre").count()
favorite_genre = user_genre_counts.withColumn("rank", row_number().over(window_spec)).filter("rank = 1")
favorite_genre.select("user_id", "genre").show(10, truncate=False)

# 9. Visualize distribution of artists across major genres
genres_to_analyze = ["pop", "rock", "metal", "hip hop"]
filtered_df = joined_df.filter(col("genre").isin(genres_to_analyze))
genre_counts_pd = filtered_df.groupBy("genre").count().toPandas()

plt.figure(figsize=(10, 6))
plt.bar(genre_counts_pd['genre'], genre_counts_pd['count'], color=['skyblue', 'salmon', 'lightgreen', 'orange'])
plt.title('Distribution of Listening Records by Major Genre')
plt.xlabel('Genre')
plt.ylabel('Total Listening Records')
plt.show()

# Part 5: In depth Analysis 

# 10. Top 10 users with the most diverse listening habits
# Calculate total listens and unique artists per user
diversity_scores = listening_df.groupBy("user_id").agg(
    count(col("artist")).alias("total_listens"),
    count(col("artist")).alias("unique_artists")
).withColumn("diversity_score", col("unique_artists") / col("total_listens"))
# A higher diversity score means more unique artists per listen
diversity_scores.sort(desc("diversity_score")).show(10)

# 11. Top 3 artists for the top 5 most popular genres
# First, get the top 5 genres
top_5_genres = top_genres.limit(5).toPandas()['genre'].tolist()
print(f"Top 5 genres are: {top_5_genres}")

# Filter for these genres and rank artists
window_spec_genre = Window.partitionBy("genre").orderBy(desc("count"))
top_artists_by_genre = joined_df.filter(col("genre").isin(top_5_genres)) \
    .groupBy("genre", "artist").count() \
    .withColumn("rank", row_number().over(window_spec_genre)) \
    .filter(col("rank") <= 3) \
    .sort(asc("genre"), desc("count"))
top_artists_by_genre.show(15, False)

# 12. Albums with the most unique artists (indicating collaborations)
collaborative_albums = listening_df.groupBy("album").agg(
    count("artist").alias("total_listens"),
    count(col("artist")).alias("unique_artists")
).filter(col("unique_artists") > 1).sort(desc("total_listens"))
print("Top 10 albums with the most unique artists:")
collaborative_albums.show(10, truncate=False)

# Part 6: Cleanup

spark.stop()
