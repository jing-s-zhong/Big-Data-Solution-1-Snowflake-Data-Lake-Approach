# Big Data Solution (1): Snowflake Data Lake Approach


## Architecture Overview

![Data Lake Solution Overview](solution_overview.jpg?raw=true "Data Lake Solution Overview")
Figure-1 Data Lake Solution Diagram

## Deploy the Solution

### 1. Clone repostary to local computer
```
git clone https://github.com/jing-s-zhong/Big-Data-Solution-1-Snowflake-Data-Lake-Approach.git
```

### 2. Customize and run deployment script
```
1_snowflake_data_lake_solution_deployment.sql
```

## Test the Solution

### 1. Setup test data lake configuration
```
2_snowflake_data_lake_demo_usage_metadata.sql
```

### 2. Manually run pipline update task
```
CALL CTRL_SCHEMA_UPDATER('WORK');
```

### 3. Generate dummy data
```
3_snowflake_data_lake_demo_usage_dummy_data.sql
```

### 4. Manually run dummy data import task
```
CALL CTRL_TASK_SCHEDULER('DATA_LOADER','WORK');
```

### 5. Check loaded data

### 6. Check presentation data

### 7. Manually run version data update task
```
CALL CTRL_TASK_SCHEDULER('DATA_VERSION','WORK');
```

### 8. Check presentation data change


## Clean-up Test Stuff

### 1. Clear the configuration import table
```
TRUNCATE TABLE CTRL_IMPORT;
```

### 2. Manuallt run pipeline update task
```
CALL CTRL_SCHEMA_UPDATER('WORK');
```

### 3. Check data lake object to make them gone
