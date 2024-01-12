export AIRFLOW_HOME=~/airflow
pip3 install apache-airflow
pip3 install typing_extensions
# initialize the database
airflow initdb

# start the web server, default port is 8080
airflow webserver -p 8080
# start the scheduler. I recommend opening up a separate terminal #window for this step
airflow scheduler

# visit localhost:8080 in the browser and enable the example dag in the home page

airflow users  create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin