# User Engagement Data Pipeline

## Table of Contents

- [Introduction](#introduction)
- [Input and Output](#input-and-output)
- [Processing Steps](#processing-steps)
- [Data Processing Strategies](#data-processing-strategies)
- [Architecture Overview](#architecture-overview)
- [Usage](#usage)
- [Contact](#contact)

## Introduction

The User Engagement Data Pipeline is designed to process user engagement data from various sources. The pipeline reads raw data, transforms it, categorizes user engagement by different types of applications, and finally saves the processed data for further analysis.

## Input and Output

- **Input**: JSON files containing the following fields:
  - `_id`
  - `_index`
  - `_score`
  - `_source`
  - `_type`

![alt text](img/root-data.png)

- **Output**: CSV files or database tables containing the following fields:
  - `Contract`
  - `Total_Giải_Trí`
  - `Total_Phim_Truyện`
  - `Total_Thiếu_Nhi`
  - `Total_Thể_Thao`
  - `Total_Truyền_Hình`
  - `MostWatch`
  - `Taste`
  - `Active_Day`

![Add](img/most_watch.png)
![Data after transforming: clean null, add date, total duration ](img/data-transform.png)

## Processing Steps

To transform the input data into the desired output format, the following steps are performed:

1. **Read Data**: Load JSON files from the specified input directory.
2. **Categorize App Names**: Map `AppName` values to predefined categories.
3. **Summarize Data**: Group by `Contract` and category, then sum the `TotalDuration`.
4. **Pivot Data**: Transform the summarized data to have one column per category.

## Data Processing Strategies

The pipeline can process data in two main ways:

1. **Direction 1**: Read data from all files at once and then calculate. You can see in "job/etl_job_1.py".
2. **Direction 2**: Read and process each file individually, then combine all results and group by `Contract` and sum.You can see in "job/etl_job_2.py".

For efficiency and scalability, Direction 2 is preferred as it allows incremental processing of large datasets.

## Architecture Overview

### 🏛️ Architecture Overview

![Architecture Overview](img/Architecture.png)

### ETL Flow

![ETL Flow](img/etl-flow.png)

### Root Data

![Root Data](img/root-data.png)

### Data Transformation

![Data Transformation](img/data-transform.png)

### Most Watched Category

![Most Watched](img/most_watch.png)

### User Taste

![User Taste](img/taste.png)

### Active User

![Active User](img/active_user.png)

## Usage

To use the User Engagement Data Pipeline, follow these steps:

### Installation

Clone the repository:

```sh
git clone
cd user-engagement-data-pipeline
```

Install the required Python packages:

```sh
pip install -r requirements.txt
```

### Contact

For any inquiries or support, please contact:
Email: daohuyhoang9119@gmail.com
