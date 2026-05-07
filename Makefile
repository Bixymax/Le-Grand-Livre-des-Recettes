.PHONY: up down dev

up:
	docker compose up -d --build

down:
	docker compose down --rmi all
	rm -rf data/output

dev:
	docker exec -it spark-master bash