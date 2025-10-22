<h1>Хакатон от МГУ</h1>

<ul>
  <p>Команды для запуска контейнеризации:</p>
  <li>docker-compose build</li>
  <li>docker-compose up -d</li>
  <li>docker-compose exec web python djangoAdmin/manage.py migrate</li>
  <li>docker-compose exec web python djangoAdmin/manage.py createsuperuser</li>
  <li>docker-compose exec web python djangoAdmin/manage.py makemigrations (для миграций)</li>
</ul>
