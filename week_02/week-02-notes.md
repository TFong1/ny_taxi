
# Week 2 Notes

## Sites

* crontab guru -- web site that helps figure out the cron string

## Airflow Tasks

* Each airflow task should be IDEMPOTENCY
  * If you run a task multiple times, it should be the same as if it ran once.

## Jinja

Airflow supports Jinja templates

## Communicate between multiple docker compose projects

https://stackoverflow.com/questions/38088279/communication-between-multiple-docker-compose-projects

## Using pgcli with docker

Enter command:  $docker exec -it _container id_ bash
Enter python interactive mode:  $ python
Use pgcli command:  $ pgcli -h localhost -p 5432 -U root -d ny_taxi


# Reference Links

* [Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html)
* [Building a Docker Image](https://airflow.apache.org/docs/docker-stack/build.html)
* [Docker File Recipes](https://airflow.apache.org/docs/docker-stack/recipes.html)