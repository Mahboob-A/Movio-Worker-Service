
# ### Docker management commands 
 
docker-up: 
	docker compose -p movio_worker_service -f dev.yml up --build -d --remove-orphans

docker-down: 
	docker compose -p movio_worker_service -f dev.yml down

docker-down-v: 
	docker compose -p movio_worker_service -f dev.yml down -v

docker-logs: 
	docker compose -p movio_worker_service -f dev.yml logs 

docker-logs-movio: 
	docker compose -p movio_worker_service -f dev.yml logs movio-worker

docker-restart-movio: 
	docker compose -p movio_worker_service -f dev.yml restart movio-worker

docker-restart-nginx:
	docker compose -p movio_worker_service -f dev.yml restart nginx

docker-restart-worker:
	docker compose -p movio_worker_service -f dev.yml restart  movio-worker-celery-worker

docker-exec-movio: 
	docker compose -p movio_worker_service -f dev.yml exec movio-worker /bin/bash 

docker-check-movio:
	docker compose -p movio_worker_service -f dev.yml run --rm movio-worker python manage.py check --deploy 

# ## 



# ### Django Commands

makemigrations: 
	docker compose -p movio_worker_service -f dev.yml run --rm movio-worker python manage.py makemigrations

migrate: 
	docker compose -p movio_worker_service -f dev.yml run --rm movio-worker python manage.py migrate

collectstatic: 
	docker compose -p movio_worker_service -f dev.yml run --rm movio-worker python manage.py collectstatic --no-input --clear 

superuser: 
	docker compose -p movio_worker_service -f dev.yml run --rm movio-worker python manage.py createsuperuser 

# ## 