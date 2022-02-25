# rippleshot_exercise
Take-home exercise for Rippleshot

### Note 
- I removed the original README.md problem statement for confidentiality purposes
- The directory `data_warehouse` contains the parquet files of interest.
- The directory `reports` contains the reports of interest.
- The `Dockerfile` will allow you to run `pyspark` if you do not have it avaialble
on your local system.
  
### Docker and Tests
If you need to use Docker, first build the docker image `docker build --tag take-home .` 

You should only have to do this once.. unless you change the Dockerfile

Use the command `docker-compose up test` to run all unit-tests. 
The container will be tied to your local volume from which you are running the command, and will pick up your changes.

### Running your Code via Docker.
If you want to run your code through `Docker` ensure you have a `main.py` file located in
a `src` folder in the root of this repo.

Running `docker-compose up run` will then run your script at `Spark` on the `Dockerfile` picking
up your code and running it.
