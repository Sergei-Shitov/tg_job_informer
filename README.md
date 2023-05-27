# tg_job_informer

Application based on TG-bot which allow to get dayly notifications about new open job positions and get analyze of dynamics of market.

## Introdution

It is the first version of application, so it has only basic operations.
\
I will make upgrades for include some interesting analytics of job market and additional job's sources

## Idea and Concept

You make the first settings and run the application using [instruction](./instuction.md) . Then the system will represent ETL pipelines and send you information about new open positions daily. Also, you will be able to get analyze of the job market dynamic in the area of your interests

## Description of elements

Application contain a few containers with different parts.

1. **bot_app**
\
this container has Python scripts that describe wokring processes of the bot itself
\
Work of container start with [db_init](./bot_app/app/db_init.py) that create tables in the database for continious working.
\
Then [app](./bot_app/app/app.py) is running. This is the main application's scripts that describes bot's logic and all commands for user interaction.
\
In the [sources](./bot_app/app/sources/) folder I put 'support' scripts.
\
[db_actions](./bot_app/app/sources/db_actions.py) script contains object with methods that allow to interact application with database

2. **airflow**
\
This is custom container with Airflow that manage all ETL processes.
\
In the [dags](./airflow/dags/) folder I put script [etl_tasks](./airflow/dags/etl_tasks.py) that contain all functions for Data manipulations.
\
dag_* files contain description of each tasks for Airflow

3. **db containers**
\
Furthermore, when application works there are two additional containers with databases.
\
One of them is *bot_db*. This is the main database that create volume folder for storaging working data.
\
The second one is *airflow_db*. This is the database with MetaDta of Airflow.
