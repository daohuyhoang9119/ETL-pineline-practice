Input : Contract , AppName , TotalDuration

Output : Contract , TVDuration , MovieDuration , ChildDuration , RelaxDuration , SportDuration

To get output from input:

1. Read data from the source
2. Return appname to categories
3. Summarize new data (Group by contract & category & Sum)
4. Pivot data

The algorithm processes data by day/month

You will have a list of data that needs to be processed

file_name = [0,1,2,3,4,5]

=> Need 1 for loop to go through each data file and process

The problem here is to read and process all 30 files

There are 2 ways to do it:

Direction 1: Read data of 30 files, then calculate
Direction 2: Read and process each file, then combine all results and group by sum

![ETL FLOW](img/etl-flow.png)
![alt text](img/root-data.png)
![Data after transforming: clean null, add date, total duration ](img/data-transform.png)

![Add](img/most_watch.png)
