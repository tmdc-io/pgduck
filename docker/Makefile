#! make
VERSION  = 0.0.3

build:
	docker build -t rubiklabs/pgduck:$(VERSION) -f Dockerfile ..

push:
	docker push rubiklabs/pgduck:$(VERSION)

up:
	docker-compose up -d

logs:
	docker-compose logs -f

down:
	docker-compose down -v

example-data:
	@./download_data.sh
