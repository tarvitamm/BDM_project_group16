# BigData 2025 project group 16

Project [Big Data](https://courses.cs.ut.ee/2025/bdm/spring/Main/HomePage) is provided by [University of Tartu](https://courses.cs.ut.ee/).

Students: **Tarvi Tamm, Markus Müüripeal, Andero Raava**

## Queries

### **Project 1**

To run this project yourself you need to install the requirments listed below or in the [requirements.txt](./requirements.txt). The notebook with the results of the queries can be found here [project.ipynb](./mnt/project.ipynb)

### **Query 1: Compute Taxi Utilization**
Utilization is calculated as the fraction of time a taxi is occupied with passengers. This is determined by computing **idle time**, which is the gap between consecutive trips by the same taxi.

```python
window_spec = Window.partitionBy("medallion").orderBy("pickup_ts")

df_utilization = df_selected.withColumn("prev_dropoff", lag("dropoff_ts").over(window_spec))
df_utilization = df_utilization.withColumn(
    "idle_time", when(col("prev_dropoff").isNotNull(), col("pickup_ts") - col("prev_dropoff")).otherwise(None)
)
```
After computing the idle time, we calculate total occupied and idle time per taxi to determine utilization.
```python
occupied_time = df_selected.groupBy("medallion").agg(spark_sum("duration").alias("total_occupied_time"))
idle_time = df_utilization.groupBy("medallion").agg(spark_sum("idle_time").alias("total_idle_time"))

df_final_utilization = occupied_time.join(idle_time, on="medallion", how="inner")
df_final_utilization = df_final_utilization.withColumn(
    "utilization", (col("total_occupied_time") / (col("total_occupied_time") + col("total_idle_time")))
)
```

### Query 2: Average Time to Find Next Fare per Borough
This query calculates the average wait time for taxis between dropping off a passenger and picking up the next one, grouped by drop-off borough.

```python
window_spec = Window.partitionBy("medallion").orderBy("dropoff_ts")
df_selected = df_selected.withColumn("next_pickup_ts", lead("pickup_ts").over(window_spec))

df_selected = df_selected.withColumn("wait_time", col("next_pickup_ts") - col("dropoff_ts"))
df_selected = df_selected.filter(col("wait_time").isNotNull())
```
Now, we calculate the average wait time per borough.
```python
avg_wait_time = df_selected.groupBy("dropoff_borough").agg(avg("wait_time").alias("avg_wait_time"))
avg_wait_time.show()
```
### Query 3: Count of Trips that Started and Ended in the Same Borough
This query counts the number of trips where the pickup and dropoff boroughs are the same.
```python
same_borough_count = df_selected.filter(
    (col("pickup_borough") == col("dropoff_borough")) &
    (col("pickup_borough") != "Unknown")
).count()
```
### Query 4: Count of Trips that Started in One Borough and Ended in Another
This query counts cross-borough trips, where the pickup and drop-off locations are in different boroughs.
```python
different_borough_count = df_selected.filter(
    (col("pickup_borough") != col("dropoff_borough")) &
    (col("pickup_borough") != "Unknown") & 
    (col("dropoff_borough") != "Unknown")
).count()
```

## Requirements
To run this project, you need the following dependencies and versions:

### **Python Version**
- Python **3.11.10**

### **Required Libraries**
| Package     | Version  |
|------------|----------|
| `pyspark`  | Latest (Tested on Apache Spark 3.x) |
| `pandas`   | 2.0.3    |
| `geopandas`| 1.0.1    |
| `shapely`  | 2.0.7    |
| `requests` | Latest   |


## Note for Students

* Clone the created repository offline;
* Add your name and surname into the Readme file and your teammates as collaborators
* Complete the field above 
* Make any changes to your repository according to the specific assignment;
* Ensure code reproducibility and instructions on how to replicate the results;
* Add an open-source license, e.g., Apache 2.0;
* convert README in pdf
* keep one report for all projects

# Project2

## Queries

Query 1: Frequent Routes
This query maps pickup and dropoff coordinates to grid cells (using a custom function) and groups trips by these cells to count how many rides occurred between each pair. It then selects and displays the top 10 most frequent routes. In addition, it simulates a streaming environment by sliding a 30‑minute time window over the events, updating the top routes whenever they change.

Query 2: Profitable Areas
This query aims to find the most profitable areas for taxi drivers. It calculates profitability per cell by:

Computing the median profit (fare plus tip) for trips starting in the cell during the last 15 minutes.

Counting the number of empty taxis (distinct medallions from drop-offs) in that cell during the last 30 minutes.

Dividing the median profit by the number of empty taxis.
The output displays the top 10 cells with the highest profitability, along with the corresponding pickup/dropoff times and the processing delay.

## Features
- Data loading and preprocessing
- Exploratory data analysis (EDA)
- Data visualization
- Model training and evaluation (if applicable)
- Insights and conclusions drawn from the analysis

  
