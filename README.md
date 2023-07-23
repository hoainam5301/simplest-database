<div align="center">
  <a href="https://github.com/othneildrew/Best-README-Template">
    <img src="images/a.png" alt="Logo">
  </a>

  <h1 align="center">Simplest Database</h1>

  <p align="center">
    <a href="thienhang.com">View Demo</a>
    ·
    <a href="https://thienhang.com">Report Bug</a>
    ·
    <a href="https://thienhang.com">Request Feature</a>
  </p>
</div>


## Deploy with docker compose
When deploying this setup, the pgAdmin web interface will be available at port 5050 (e.g. http://localhost:5050).  

``` shell
$ docker compose up
Starting postgres ... done
Starting pgadmin ... done
```

## Add postgres database to pgAdmin
Using email and password in .env file to login to pgAdmin.
After logging in with your credentials of the .env file, you can add your database to pgAdmin. 
1. Right-click "Servers" in the top-left corner and select "Create" -> "Server..."
2. Name your connection
3. Change to the "Connection" tab and add the connection details:
- Hostname: "host.docker.internal" (this would normally be your IP address of the postgres database - however, docker can resolve this container ip by its name)
- Port: "5432"
- Maintenance Database: $POSTGRES_DB (see .env)
- Username: $POSTGRES_USER (see .env)
- Password: $POSTGRES_PW (see .env)
  
## Expected result

Check containers are running:
```
$ docker ps

```

Stop the containers with

``` shell
$ docker compose down
# To delete all data run:
$ docker compose down -v
```
