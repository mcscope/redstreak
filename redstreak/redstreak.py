import csv
import argparse
from nodes import Limit, Order, Selection, Scan, NaiveInnerJoin, Mean
from lib.profile import print_memory
from serialize import readtups

"""
RedStreak - an example Database

Written for Bradfield CS database course
Spring 2018 by Lady Red



TODO:
~ Out of Core sort
~ Projection Node
~ Convert everything to use tuples
~ Binary storage format

"""

parser = argparse.ArgumentParser()
# parser.add_argument("csv_path")
# parser.add_argument("csv_path2")

args = parser.parse_args()


def netflix_query(data):
    def netflix_filter(row):
        genres = row['genres']
        funny = "Comedy" in genres
        nerdy = "Sci-Fi" in genres
        educational = "Documentary" in genres
        return nerdy and educational or nerdy and funny

    def netflix_score(row):
        categories = {
            "Children": -3,
            "Romance": -2,
            "Documentary": 1,
            "Sci-Fi": 2,
            "Thriller": 1,
            "Fantasy": 1,
            "IMAX": 1,
        }

        genres = row['genres']
        score = 0
        for category, mod in categories.items():
            if category in genres:
                score += mod
        # lower is better
        return -1 * score

    report = Limit(
        50,
        Order(
            netflix_score,
            Selection(
                netflix_filter,
                Scan(data)
            )
        )
    )
    return report


def highly_rated(movies, ratings):
    def rating_high_to_low(row):
        return -1 * row['Mean']

    report = Limit(
        50,
        Order(
            rating_high_to_low,
            NaiveInnerJoin(
                'movieId',
                Mean(
                    "rating",
                    Scan(ratings),
                    group_by="movieId"
                ),
                Scan(movies),
            ),
        )
    )
    return report


def run_query(movies, ratings):
    """
    A place to manually program queries
    """
    report = highly_rated(movies, ratings)

    print(report.explain())
    print("REDSTREAK RESULT:")
    print("-" * 80)
    print(f"# : {'Title':39.39}  |{'Genres':39.39}")

    print("-" * 80)
    for i, row in enumerate(report):
        buff = []
        for key, value in row.items():
            if key == 'timestamp' or "Id" in key:
                continue
            buff.append(f"{value:3.30}")
        print(i, " | ".join(buff))
        # print(f"{i:<2}: {row['title']:39.39} |{row['genres']:39.39}")


def main():
    # TODO remove
    csv_path = "../../ml-20m/movies.csv"
    # csv_path2 = "../../ml-20m/ratings.csv"
    csv_path2 = "100kratings.csv"

    with open(csv_path, newline='') as csvfile:
        with open(csv_path2, newline='') as csvfile2:

            run_query(readtups(file), readtups(file))


if __name__ == '__main__':
    main()
