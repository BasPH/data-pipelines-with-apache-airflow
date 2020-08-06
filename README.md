# Data Pipelines with Apache Airflow

Code accompanying the Manning book [Data Pipelines with Apache Airflow](https://www.manning.com/books/data-pipelines-with-apache-airflow).

### Structure

Overall, this repository is structured as follows:

```
├── CHANGELOG.md          # Changelog detailing updates to the code.
├── LICENSE
├── Makefile              # Helper commands.
├── README.md             # This readme.
├── chapters              # Code examples for each of the Chapters.
├── docker                # Supporting Docker image (containing Airflow).
└── environment.yml
```

The most interesting parts are probably the *docker* directory and the *chapter* directories under *chapters*.

The *docker* directory contains a custom Airflow image that will be used through out the book.

The *chapter* directories contain the code examples for each specific Chapter. Code for each Chapter is generally structured something like follows:

```
├── dags                  # Airflow DAG examples (+ other code).
├── docker-compose.yml    # Docker-compose file used for running the Chapter's containers.
└── readme.md             # Readme with Chapter-specific details, if any.
```

### Usage

Details for running specific chapter examples are available in the corresponding chapter's readme. In general, most code examples are run using docker-compose, together with the provided docker-compose.yml file in each chapter. This docker-compose file will take care of spinning up the required resources and start an Airflow instance for you. Once everything is running, you should be able to run the examples in Airflow using your local browser.

Some later Chapters (such as Chapters 11 and 13) may require a bit more setup. The details for doing so are described in the corresponding readme's and in the Chapter's themselves.
