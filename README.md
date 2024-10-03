# Hot Spot Analysis Project

This project implements a spatial data analysis to identify "hot zones" and "hot cells" in a dataset of taxi pickup points. The analysis is performed using Apache Spark and Scala, involving two main components: **Hot Zone Analysis** and **Hot Cell Analysis**.

## Project Overview

The goal of this project is to perform geospatial analysis to identify areas with dense taxi pickups using distributed computing. The project is divided into two sections:

1. **Hot Zone Analysis**: Identifies spatial zones with the highest number of taxi pickups.
2. **Hot Cell Analysis**: Calculates Z-Scores to find statistically significant clusters of high or low pickup counts.

### Technology Stack

- **Apache Spark**: Distributed data processing framework.
- **Scala**: Programming language used for implementation.
- **SQL Queries**: Used to perform aggregation and filtering on large datasets.
- **Spark DataFrames**: Utilized for structured data manipulation and SQL-style querying.
- **User-Defined Functions (UDFs)**: Created to extend the capabilities of SQL and DataFrame queries.
- **Geospatial Analysis**: Includes calculating Z-Scores for hot cell analysis.

## Implementation Details

### 1. Hot Zone Analysis

This section performs a **spatial range join** to associate taxi pickup points with predefined rectangular zones. The key steps involved are:

- **Spatial Range Join**: Using the `ST_Contains` function, pickup points are matched to the respective rectangular zones.
- **Aggregation**: The number of pickups in each zone is calculated, and the zones are ranked by "hotness" (i.e., pickup count).
- **Sorting**: The result is sorted in ascending order based on the pickup count, with the "hottest" zones appearing first.
- **SQL Queries**: SQL-style queries using Spark’s DataFrame API are used to perform aggregation and filtering.

**Key Functionality**:
- `ST_Contains(rectangle, point)`: Determines whether a pickup point lies within a given rectangular zone by checking the point's coordinates against the boundaries of the rectangle.

### 2. Hot Cell Analysis

In the Hot Cell Analysis, a Z-Score is calculated for each cell in a spatial grid to find areas with statistically significant pickup activity. The steps include:

- **Data Preparation**: Pickup points are assigned to cells based on their coordinates (latitude, longitude, and potentially time).
- **Grid Division**: The spatial area is divided into a grid of cells, each with unique `x, y, z` coordinates.
- **Z-Score Calculation**: For each cell, a Z-Score is computed using the formula for spatial statistics. This score measures the intensity of pickup counts compared to neighboring cells.
- **SQL Queries and UDFs**: SQL-style queries, extended by User-Defined Functions (UDFs), are used for data transformations and Z-Score calculations within the DataFrames.

**Z-Score Formula**:
Z = (x_i - μ) / σ

Where:
- `x_i` is the number of pickups in the cell,
- `μ` is the overall mean pickup count,
- `σ` is the standard deviation of the counts.

## Lessons Learned

- **Spark and Scala**: Gained proficiency in spatial queries, data handling with DataFrames, SQL-style queries, and functional programming in Scala. Improved understanding of how to use Apache Spark for large-scale data processing.
- **Geospatial Analysis**: Learned geospatial querying, statistical analysis (Z-Score), and their practical applications in distributed environments.
- **Troubleshooting**: Overcame technical challenges such as environment setup, and permissions issues that prevented code execution, leading to valuable debugging experience.


## How to Run the Project

1. **Prerequisites**:
    - Apache Spark installed and configured.
    - IntelliJ IDEA with the Scala plugin (recommended for development).
  
2. **Running the Project**:
    - Ensure the dataset is loaded and properly formatted.
    - Use `spark-submit` to run the Scala program, or configure your IDE to execute the main class.

3. **Expected Output**:
    - **Hot Zone Analysis**: Sorted list of zones based on pickup count.
    - **Hot Cell Analysis**: Dataframe of cells with calculated Z-Scores.
