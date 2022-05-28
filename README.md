sudo docker run -d --name rabbitmq -p 5672:5672 -p 8080:15672 \
--restart=unless-stopped \
-e RABBITMQ_DEFAULT_USER=root \
-e RABBITMQ_DEFAULT_PASS=test123# \
rabbitmq:management