#! make
VERSION  = 0.0.1-dev

build:
	docker build -t rubiklabs/flash:$(VERSION) -f Dockerfile ..

push:
	docker push rubiklabs/flash:$(VERSION)

up:
	docker-compose up -d

logs:
	docker-compose logs -f

down:
	docker-compose down -v

example-data:
	@./download_data.sh
