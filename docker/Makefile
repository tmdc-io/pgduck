#! make

build:
	docker build -t rubiklabs/pgduck:0.0.3 -f Dockerfile ..

up:
	docker-compose up -d

logs:
	docker-compose logs -f

down:
	docker-compose down -v

example-data:
	@./download_data.sh
