from pyspark import SparkConf, SparkContext
import sys

# GET MOVIE SIMILARITIES USING JACCARD INDEX


def load_movie_names():
    movie_names = {}
    with open('./data.item', encoding="ISO-8859-1") as f:
        for line in f:
            line_data = line.split('|')
            movie_names[int(line_data[0])] = line_data[1]
    return movie_names


def filter_duplicates(user_movies_seen):
    # userID => ((movieID1, count1), (movieID2, count2))
    movies_seen = user_movies_seen[1]
    (movie1, count1) = movies_seen[0]
    (movie2, count2) = movies_seen[1]
    return movie1 < movie2


def compute_jaccard_index(movie_pairs):
    # ((movieID1, count1), (movieID2, count2)) => list_of_users
    pair = movie_pairs[0]
    list = movie_pairs[1]
    num_of_pairs = len(list)
    count_0 = pair[0][1]
    count_1 = pair[1][1]
    movie_0 = pair[0][0]
    movie_1 = pair[1][0]
    jaccard = num_of_pairs / (count_0 + count_1 - num_of_pairs)
    return (movie_0, movie_1), (jaccard, num_of_pairs)


def get_movie_counts(mov):
    # (movie ID => list_of_users)
    movieID = mov[0]
    list_of_users = mov[1]
    return (movieID, len(list_of_users)), list_of_users


# Use all cores of PC
conf = SparkConf().setMaster("local[*]").setAppName("movie-similarities")
sc = SparkContext(conf=conf)

print("\nLoading movie names...")
name_dict = load_movie_names()

data = sc.textFile("u.data")

# Emit every movie rated together by the same user & Map ratings to key/value pairs: (user ID => movie ID, rating)
rdd_movies = data.map(lambda l: l.split()).map(lambda l: (int(l[0]), (int(l[1]), float(l[2]))))

# Filter bad ratings (star rating: 1-5 stars)
filtered_movies = rdd_movies.filter(lambda x: x[1][1] > 1)

# Remove rating data since we will not use it anymore: (user ID => movie ID)
movies = filtered_movies.map(lambda x: (x[0], x[1][0]))

# Flip: (movie ID => user ID)
movies = movies.map(lambda x: (x[1], x[0]))

movie_with_list_of_users = movies.map(lambda x: (x[0], [x[1]])).reduceByKey(lambda x, y: x + y)
# Now: (movie ID => list_of_users)

movie_counts = movie_with_list_of_users.map(get_movie_counts)
# Now: ((movie ID, movie_counts) => list_of_users)

pairs = movie_counts.flatMapValues(lambda v: v).map(lambda x: (x[1], x[0]))
# Now: ((user ID => (movie_ID, count))

# Self-join to find every combination: RDD consists of pairs: userID => ((movieID1, count1), (movieID2, count2))
joined_pairs = pairs.join(pairs)

# Filter out duplicate pairs
unique_joined_movies = joined_pairs.filter(filter_duplicates)

# Flip: ((movieID1, count1), (movieID2, count2)) => userID
unique_joined_movie_pairs = unique_joined_movies.map(lambda x: (x[1], x[0]))

# Collect all users that have seen both movies: ((movieID1, count1), (movieID2, count2)) => list_of_users
movie_pairs = unique_joined_movie_pairs.map(lambda x: (x[0], [x[1]])).reduceByKey(lambda x, y: x + y)

movie_pair_similarities = movie_pairs.map(compute_jaccard_index).cache()
# Now: ((movieID1, movieID2) => (jaccard_index, num_of_pairs)

# Save the results if desired
# movie_pair_similarities.sortByKey()
# movie_pair_similarities.saveAsTextFile("movie-similarities")

# Extract similarities for the movie we care about that are "good".
if len(sys.argv) > 1:

    scoreThreshold = 0.3
    coOccurenceThreshold = 10

    movieID = int(sys.argv[1])

    # Filter for movies with this sim that are "good" as defined by the quality thresholds above
    filteredResults = movie_pair_similarities.filter(lambda pairSim: \
        (pairSim[0][0] == movieID or pairSim[0][1] == movieID)
        and pairSim[1][0] > scoreThreshold
        and pairSim[1][1] > coOccurenceThreshold)

    # Sort by quality score
    results = filteredResults.map(lambda pairSim: (pairSim[1], pairSim[0])). \
        sortByKey(ascending=False). \
        take(10)

    print("Top 10 similar movies for " + name_dict[movieID])
    for result in results:
        (sim, pair) = result
        # Display the similarity result that isn't the movie we're looking at
        similarMovieID = pair[0]
        if similarMovieID == movieID:
            similarMovieID = pair[1]
        print(name_dict[similarMovieID] + "\tscore: " + str(sim[0]) + "\tstrength: " + str(sim[1]))

#e.g. for 'Star Wars (1977)' movie:
# spark-submit movie-similarites.py 50
