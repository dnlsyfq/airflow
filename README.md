# Airflow 

```
dag = DAG('my_dag',start_date = datetime(2020,12,1))
start_cluster = StartClusterOperator(task_id="start_cluster",dag=dag)
input_athlete_data = SparkJobOperator(task_id="input_athlete_data",dag=dag)
input_venue_data = SparkJobOperator(task_id="input_venue_data",dag=dag)

start_cluster.set_downstream(input_athlete_data)
input_athlete_data.set_downstream(enrich_athlete_data)
input_venue_data.set_downstream(enrich_athlete_data)
```


```
response = requests.get('
https://api.themoviedb.org/3/discover/movie?api_key=' + api_key + '&primary_release_year=2017&sort_by=revenue.desc')

response.json()
```

