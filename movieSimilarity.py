import sys, time
from pyspark import SparkConf, SparkContext
from math import sqrt


score_threshold = 0.97
co_occurence_threshold = 50
movie_id = 50
sim_strength_cut = 200


def load_movie_name():
    movie_names = {}

    with open("data/ml-100k/u.item", encoding='ascii', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movie_names[int(fields[0])] = fields[1]

    return movie_names


def filter_duplicates(user_ratings):
    ratings = user_ratings[1]
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]

    return movie1 < movie2


def make_pairs(user_ratings):
    ratings = user_ratings[1]
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]

    return (movie1, movie2), (rating1, rating2)


def compute_cosine_similarity(ratings_pairs):
    num_pairs = 0
    sum_xx = sum_yy = sum_xy = 0

    for ratingX, ratingY in ratings_pairs:
        sum_xx += ratingX * ratingX
        sum_yy += ratingY * ratingY
        sum_xy += ratingX * ratingY
        num_pairs += 1

    numerator = sum_xy
    denominator = sqrt(sum_xx) * sqrt(sum_yy)

    score = 0

    if denominator:
        score = numerator/float(denominator)

    return score, num_pairs


def main():
    print("\nLoading movie names")
    name_dict = load_movie_name()
    print("Movie ID and name dict made")

    time.sleep(3)

    data = sc.textFile("data/ml-100k/u.data")

    ratings = data.map(lambda l: l.split()).map(lambda l: (int(l[0]), (int(l[1]), float(l[2]))))
    print("key-value pairs made for data")

    time.sleep(3)
    joined_ratings = ratings.join(ratings)
    unique_joined_ratings = joined_ratings.filter(filter_duplicates)

    print("Pairs joined and unique for user")
    time.sleep(3)

    movie_pairs = unique_joined_ratings.map(make_pairs)
    movie_pair_ratings = movie_pairs.groupByKey()
    print("Pairs joined and unique for user")
    time.sleep(3)

    movie_pair_similarity = movie_pair_ratings.mapValues(compute_cosine_similarity).cache()
    filtered_results = movie_pair_similarity.filter(lambda pair_sim: (pair_sim[0][0] == movie_id or pair_sim[0][1] == movie_id) and pair_sim[1][0] > pair_sim[1][1] > co_occurence_threshold)

    results = filtered_results.map(lambda pair_sim: (pair_sim[1], pair_sim[0])).sortByKey(ascending=False).take(10)

    print("Top 10 similar movies for " + name_dict[movie_id])

    for result in results:
        (sim, pair) = result
        similar_movie_id = pair[0]

        if similar_movie_id == movie_id:
            similar_movie_id = pair[1]

        print(name_dict[similar_movie_id] + "\tscore:" + str(sim[0]) + "\tstrength:" + str(sim[1]))


if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("movieSimilarity")
    sc = SparkContext(conf=conf)
    main()
    sc.stop()
    print("\nDone")
