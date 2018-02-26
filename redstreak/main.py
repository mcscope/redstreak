import csv
import argparse
from nodes import Limit, Selection, Scan, Mean
from redstreak.serialize import FileScan

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


def simple_record_query(data):
    report = Limit(
        50,
        Selection(lambda row: "War" in row.genres,
                  data
                  )
    )
    return report


# def highly_rated(movies, ratings):
#     def rating_high_to_low(row):
#         return -1 * row['Mean']

#     report = Limit(
#         50,
#         Order(
#             rating_high_to_low,
#             NaiveInnerJoin(
#                 'movieId',
#                 Mean(
#                     "rating",
#                     Scan(ratings),
#                     group_by="movieId"
#                 ),
#                 Scan(movies),
#             ),
#         )
#     )
#     return report


def run_query(report):
    """
    A place to manually program queries
    """

    print(report.explain())
    print("REDSTREAK RESULT:")
    print("-" * 80)
    print(f"# : {'Title':39.39}  |{'Genres':39.39}")

    print("-" * 80)
    for i, row in enumerate(report):
        print(f"{i} {row}")


def main():
    run_query(
        simple_record_query(FileScan("Movie")))


if __name__ == '__main__':
    main()
