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

# Task 4.a.1
pipeline = [
    {
        '$group': {
            '_id': '$name',
            'total': {
                '$sum': 1
            }
        }
    }, {
        '$sort': {
            'total': -1
        }
    }, {
        '$limit': 10
    }
]

ans = comments.aggregate(pipeline)


# def user_name(obj):
#     return obj['_id']
#
#
# usernames = map(user_name, ans)

usernames = []
for i in ans:
    usernames.append(i['_id'])
print(usernames)

# Task 4.a.2
pipeline2 = [
    {
        '$group': {
            '_id': '$movie_id',
            'total': {
                '$sum': 1
            }
        }
    }, {
        '$sort': {
            'total': -1
        }
    }, {
        '$limit': 10
    }, {
        '$lookup': {
            'from': 'movies',
            'localField': '_id',
            'foreignField': '_id',
            'as': 'data'
        }
    }, {
        '$unwind': {
            'path': '$data',
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$project': {
            'data.title': 1
        }
    }
]

ans2 = comments.aggregate(pipeline2)
# for i in ans2:
#     print(i)

movies_name = []
for i in ans2:
    movies_name.append(i['data']['title'])
print(movies_name)

