set -ex
# API test
http http://0.0.0.0:8080/api/service1
http http://0.0.0.0:8080/api/service2
http http://0.0.0.0:8080/api/service3
http http://0.0.0.0:8080/api/grouped
