# Use Of PySpark For Movie Similarities With Jaccard Index 

## Dataset
The dataset is the MovieLens 100K Dataset that can be found [here](https://grouplens.org/datasets/movielens/). It includes 100,000 ratings from 1000 users on 1700 movies and was released 4/1998. The needed files for the program are uploaded with changed name.

## Requirements
- PySpark

## Example Usage
To find similar movies for 'Star Wars (1977)' movie:
```
spark-submit movie-similarites.py 50

```