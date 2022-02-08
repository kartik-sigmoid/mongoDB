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
     {
         "$geoNear": {
             "near": {"type": "Point", "coordinates": [-84.526169, 37.986019] },
             "maxDistance":10*10000000000000,
             "distanceField": "dist.calculated",
             "includeLocs": "dist.location",
             "distanceMultiplier":1/1000,
             "spherical": "true"
      }
     },
         {"$project": {"city": "$location.address.city", "distance": "$dist.calculated"}},
         {"$group": {"_id": {"distance": "$distance", "city": "$city"} }},
         {"$sort": {"_id.distance": 1}},
         {"$limit": 10}
    ]
    li = collections.aggregate(pipeline)
    res = []
    for i in li:
        res.append(i)
    return res


def task_two_2(theaters):
    pipeline2 = [
        {
            '$geoNear': {
                'near': {
                    'type': 'Point',
                    'coordinates': [
                        -118.11414, 37.667957
                    ]
                },
                'maxDistance': 1000000,
                'distanceField': 'dist.calculated',
                'includeLocs': 'dist.location',
                'distanceMultiplier': 0.001,
                'spherical': True
            }
        }, {
            '$project': {
                'theaterId': 1,
                '_id': 0,
                'city': '$location.address.city',
                'distance': '$dist.calculated'
            }
        }, {
            '$limit': 10
        }
    ]

    results2 = theaters.aggregate(pipeline2)
    for result in results2:
        print(f"City - {result['city']} ; TheaterId - {result['theaterId']} ; Distance - {result['distance']}")


def queries(theaters):
    # print('top 10 cities with max. of theaters')
    # taskOne = task_one(theaters)
    # print(taskOne)

    print("top 10 theatres nearby given coordinates")
    co_ordinates = [-85.76461, 38.327175]

    taskTwo = task_two_2(theaters)
    print(taskTwo)


if __name__ == "__main__":
    client = MongoClient("mongodb://localhost:27017")

    # collection
    theaters = client.shop3.theaters

    queries(theaters)

