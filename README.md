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

1. **Data Import Query:** Loads raw data from external sources into a working dataset.  
2. **Data Cleaning Query:** Handles missing values, removes duplicates, and standardizes data types to prepare the dataset for analysis.  
3. **Exploratory Data Analysis Query:** Computes summary statistics and visualizes distributions to uncover patterns and trends within the data.  
4. **Aggregation Query:** Groups and summarizes data by relevant categories to highlight key metrics and trends.  
5. **Visualization Query:** Creates various plots and charts to visually communicate data relationships and insights.  
6. **Model Training Query:** Splits the data into training and testing sets, applies machine learning algorithms, and tunes model parameters.  
7. **Model Evaluation Query:** Assesses model performance using appropriate metrics, such as accuracy, precision, recall, or RMSE.

## Features
- Data loading and preprocessing
- Exploratory data analysis (EDA)
- Data visualization
- Model training and evaluation (if applicable)
- Insights and conclusions drawn from the analysis

  # Project 4: Airline Delay and Cancellation Prediction

This project focuses on analyzing and preparing flight data for the purpose of predicting airline cancellations using Apache Spark. We leverage data engineering, feature engineering, and exploratory data analysis techniques to prepare the dataset for machine learning modeling.

---

## Part 1: Data Ingestion and Setup

- Loaded raw 2009 airline data into Spark.
- Converted to a structured DataFrame with inferred schema.
- Saved the data in Parquet format, partitioned by `Year` and `Month` for efficient processing.

---

## Part 2: Data Cleaning and Preprocessing

- Removed diverted flights.
- Handled missing values in key columns.
- Renamed relevant columns.
- Added derived features such as `DayOfWeek` and `Month`.
- Saved the clean dataset as partitioned Parquet files.

---

## Part 3: Exploratory Data Analysis (EDA)

- Analyzed carrier distributions and cancellation reasons.
- Visualized:
  - Top 10 carriers
  - Cancellation reason distribution
  - Class imbalance between cancelled and non-cancelled flights

**Key Finding:**  
Cancellation reasons A and B were the most common. Cancellations are a minority class, highlighting a class imbalance problem.

---

## Part 4: Feature Engineering

- Encoded categorical features using `StringIndexer` and `OneHotEncoder`.
- Combined all features into a single vector using `VectorAssembler`.
- Prepared the dataset for modeling.

---

## Part 5: Modeling

- Trained four classification models:
  - Logistic Regression
  - Decision Tree
  - Random Forest
  - Gradient-Boosted Trees (GBT)
- Used a 70/30 train-test split and sampling to reduce computational load.
- Applied 3-fold cross-validation for hyperparameter tuning.

**Evaluation Metrics:**
- Area Under ROC Curve (AUC)
- Accuracy

**Key Finding:**  
GBT achieved the best predictive performance, making it the most effective model for flight cancellation prediction.

---

## Part 6: Model Explainability

- Used the `featureImportances` attribute of GBT to interpret predictions.
- Most influential features:
  - `Month`
  - `DayOfWeek`
  - `CRS_DEP_TIME`
  - `FlightNum`
  - Route-related features (e.g., `DEST_Vec_LGA`, `ORIGIN_Vec_LGA`)

**Key Insight:**  
Both schedule- and route-related variables significantly affect cancellation likelihood, reflecting real-world patterns like seasonal demand and airport congestion.

---

## Part 7: Model Persistence and Inference

- Combined the best model (GBT) with the preprocessing pipeline and saved it using Spark’s `PipelineModel.write()`.
- Reloaded and applied the model on 2010 flight data using the same data preparation steps.
- Generated predictions with raw scores and class probabilities.

**Key Finding:**  
The model achieved:
- **AUC**: 0.692  
- **Accuracy**: 0.982  
on 2010 data, showing consistent performance across different years.

---