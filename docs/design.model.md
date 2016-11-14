# HSBCBackend 

Project design and algorithms

## Modules

Source modules are in `/src`.

### DB

Use wrapper classes of `pymongo` database and collection to more conveniently manipulate db.
A reason to include this is to use a decorator to avoid upgrade issues from `pymongo` package.

* `mongodb.py` includes `MongoDB` and `MongoCollection` class, they basically wrap functions that are frequently used in `pymongo`

* `document_db.py` includes a abstract class for document-oriented database.

**TODO**: Fill up document_db.py incase of database migration

### preprocess

This package includes all the pre-processing helpers and classes.

* `weather_data_helper.py` includes helpers that fetch weather data from hk websites and store them into database. A high level API that should be used is `fetch_and_store_weather_data(is_forecast)`.

* `tasks.py` is a module script that you can directly run to perform preprocessing tasks.

**TODO**: Restructure the design, i.e., put the data collecting tasks to other places.

### query

Under developed. For now, it should contain convenient queries to be used by fore-end.

### utlis

Basic utilities used across the project. Some useful ones include:

* `safe_open_url`: wrap urllib functions to request file on internet safely; when exceptions raised, they are logged.
* `parse_json_file`: parse json like file either from local or from internet.
* `camel2snake`: a small util converting Camel case strings to snake case.
* `task_thread`: create a thread that periodically calls your task function.

**TODO**: Current utils are limited. When utils gets a lot, we should restructure it.

### config

This directory includes all the configuration files that this project uses.

To config database names and collection names, you can modify db_config.json



