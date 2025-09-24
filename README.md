# PySpark Music Data Analysis Project

This repository contains a data analysis project using **PySpark** to process a large music dataset from Last.fm. The project was initially completed as a guided course on Coursera and has been expanded to demonstrate a comprehensive data analysis workflow.

## Project Overview

The goal of this project was to apply PySpark to a real-world dataset that's too large for traditional single-machine processing. The project involves a complete data analysis lifecycle, including:

* **Data Ingestion and Cleaning:** Loading a 1 GB dataset and preparing it for analysis by handling data types and potential inconsistencies.
* **Exploratory Data Analysis (EDA):** Performing a series of queries to understand user behavior, artist popularity, and genre trends.
* **Data Aggregation and Transformation:** Combining information from multiple datasets to derive new insights.
* **Advanced Analysis & Visualization:** Answering complex questions and creating insightful charts to communicate key findings.

## Dataset

The analysis is performed on two datasets from Last.fm:

* `listening.csv`: Contains records of user listening activity (user_id, track, artist, album). This file is approximately 1 GB.
* `genre.csv`: Contains genre information for various artists (artist, genre).

## Key Analysis Questions

This project answers a series of questions to uncover trends in the music listening data:

1.  **User-Centric Analysis:**
    * Find all records of users who have listened to a specific artist (Rihanna).
    * Identify the top 10 users who are the biggest fans of Rihanna.
    * Determine the top 10 users who are fans of Pop music.

2.  **Music Popularity Analysis:**
    * Identify the top 10 most famous tracks, albums, and artists.
    * Find the top 10 most famous tracks by a specific artist (Rihanna).
    * Discover the top 10 most famous genres.

3.  **Advanced Insights:**
    * Find each user's favorite genre based on their listening history.
    * Analyze the distribution of artists across major genres like Pop, Rock, Metal, and Hip Hop and visualize the results.

## Technologies Used

* **PySpark:** For efficient distributed data processing.
* **Google Colab:** The environment used for development, leveraging its free access to powerful compute resources for big data.
* **Matplotlib:** For creating static visualizations.
* **Pandas:** For converting aggregated Spark DataFrames into a format suitable for plotting.

## How to Run the Project

1.  **Environment:** The notebook is designed to run in Google Colab.
2.  **Mount Google Drive:** The large `listening.csv` file is stored on Google Drive. You will need to mount your Drive to the Colab environment using the code in the notebook.
3.  **Data Path:** Update the file path in the notebook to match the location of your `listening.csv` and `genre.csv` files on your Google Drive.
4.  **Run Cells:** Execute the cells in the `pyspark_music_analysis.py` script sequentially to replicate the analysis.
