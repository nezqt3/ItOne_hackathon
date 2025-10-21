<h1>Хакатон от МГУ</h1>

Команды для запуска контейнеризации:
docker-compose build
docker-compose up -d
docker-compose exec web python djangoAdmin/manage.py migrate
docker-compose exec web python djangoAdmin/manage.py createsuperuser
