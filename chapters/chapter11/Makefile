
build: build-api build-ranking build-api

build-fetch:
	docker build -t airflowbook/movielens-fetch images/movielens-fetch

build-ranking:
	docker build -t airflowbook/movielens-rank images/movielens-rank

build-api:
	docker build -t airflowbook/movielens-api images/movielens-api

build-wttr:
	docker build -t airflowbook/wttr-example images/wttr-example

run-wttr: build-wttr
	docker run airflowbook/wttr-example Amsterdam
