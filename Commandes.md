docker-compose up -d --build
docker-compose down

py create_topic.py
py producer.py
py consumer.py

docker exec -it mongodb mongosh -u admin -p admin123
use amazon_reviews
db.raw_reviews.countDocuments()
db.raw_reviews.findOne()

bash spark/submit.sh
db.predictions.countDocuments()
db.predictions.findOne()
db.predictions.aggregate([
...     { $group: { _id: "$predicted_label", count: { $sum: 1 } } }
... ])

docker-compose up -d --build flask-app
py app.py