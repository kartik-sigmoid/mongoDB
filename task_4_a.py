from pymongo import MongoClient


def task_one(comments):
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
    usernames = []
    for i in ans:
        usernames.append(i['_id'])
    return usernames


def task_two(comments):
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

    new_data = comments.aggregate(pipeline2)
    movies_name = []
    for i in new_data:
        movies_name.append(i['data']['title'])
    return movies_name


def task_three(comments,year):
    pipeline = [
        {"$project": {"_id": 0, "date": {"$toDate": {"$convert": {"input": "$date", "to": "long"}}}}},
        {"$group": {
            "_id": {
                "year": {"$year": "$date"},
                "month": {"$month": "$date"}
            },
            "total_person": {"$sum": 1}}
        },
        {"$match": {"_id.year": {"$eq": year}}},
        {"$sort": {"_id.month": 1}}
    ]
    result = comments.aggregate(pipeline)
    li = []
    for i in result:
        li.append(i)
    return li


def queries(comments):
    print('Find top 10 users who made the maximum number of comments')
    taskOne = task_one(comments)
    print(taskOne)

    print('Find top 10 movies with most comments')
    taskTwo = task_two(comments)
    print(taskTwo)

    print("All comments with given year i.e. 2000")
    year = "2000"
    taskThree = task_three(comments, 2000)
    print(taskThree)


if __name__ == "__main__":
    client = MongoClient("mongodb://localhost:27017")

    # collection
    comments = client.shop3.comments

    queries(comments)

