from pymongo import MongoClient


def task_one(theaters):
    pipeline = [
        {"$group": {"_id": {"city": "$location.address.city"}, "total_theaters": {"$sum": 1}}},
        {"$sort": {"total_theaters": -1}},
        {"$limit": 10},
        {"$project": {"city_name": "$_id.city", "_id": 0, "total_theaters": 1}}
    ]
    li = theaters.aggregate(pipeline)
    res = []
    for i in li:
        res.append(i)
    return res


def task_two(collections, coord):
    pipeline = [
        {"$group": {"_id": {"city": "$location.address.city"}}},
        {"$match": {"location.geo.coordinates[0]": coord[0], "location.geo.coordinates[1]": coord[1]}},
        {"$limit": 10},
        {"$project": {"city_name": "$_id.city", "_id": 0}}
    ]
    li = collections.aggregate(pipeline)
    res = []
    for i in li:
        res.append(i)
    return res


def queries(theaters):
    print('top 10 cities with max. of theaters')
    taskOne = task_one(theaters)
    print(taskOne)

    print("top 10 theatres nearby given coordinates")
    co_ordinates = [-85.76461, 38.327175]
    taskTwo = task_two(theaters, co_ordinates)
    print(taskTwo)


if __name__ == "__main__":
    client = MongoClient("mongodb://localhost:27017")

    # collection
    theaters = client.shop3.theaters

    queries(theaters)

