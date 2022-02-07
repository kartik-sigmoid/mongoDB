import pymongo
from pprint import pprint
from pymongo import MongoClient

# Task 1
client = MongoClient("mongodb://localhost:27017")

# Task 3
movies = client.shop3.movies
comments = client.shop3.comments
theaters = client.shop3.theaters
users = client.shop3.users

new_movie = {
    "plot": "slice of life",
    "genres": ["dark", "supernatural"],
    "title": "shut"
}

new_comment = {
    "name": "Gojo Saturu",
    "text": "Dying to win and risking death to win is completely different.",
}

new_theater = {
    "theater_id": 73,
    "location": {
        "address": {
            "city": "village hidden in leaf"
        }
    }
}

new_user = {
    "name": "Draken",
    "email": "delinquent@sloppy.world.com",
    "password": "asIfIWillTellYa"
}

# Insertion
new_movie_id = movies.insert_one(new_movie).inserted_id
new_comment_id = comments.insert_one(new_comment).inserted_id
new_theater_id = theaters.insert_one(new_theater).inserted_id
new_user_id = users.insert_one(new_user).inserted_id

# Fetching new documents
movie = movies.find_one({"_id": new_movie_id})
comment = comments.find_one({"_id": new_comment_id})
theater = theaters.find_one({"_id": new_theater_id})
user = users.find_one({"_id": new_user_id})

# Printing newly added documents
pprint(movie)
print(movie['plot'])
pprint(comment['text'])
pprint(theater['location']['address']['city'])
pprint(user['email'])


