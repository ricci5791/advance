# Advance

In order to start container with PySpark job run next command (you should be in the same directory as docker-compose.yml file):

`docker-compose up`

---

To get an understanding of what is going on you can visit Spark UI, which is usually hosted on `localhost:4040`

---

Main task that is performed by the Spark here is to calculate various data insights about YELP dataset (https://www.yelp.com/dataset)

(For simplifying datasets and reducing disk usage datasets were cutted down by hand)

---

In order to get full utilization of computing power it is reccomended to download yelp dataset, place it in dataset folder and start container with 
`docker-compose up --build` command

---

To see results of calculation go to "0.0.0.0:9001", sign in with credentials that are 
in docker-compose.yml environment and go to `testbucket` bucket. (Keep attention that credentials are quoted)