import csv
import argparse
import nodes

"""
RedStreak - an example Database

Written for Bradfield CS database course
Spring 2018 by Lady Red


"""

parser = argparse.ArgumentParser()
parser.add_argument("csv_path")

args = parser.parse_args()


def run_query(data):
    """
    A place to manually program queries
    """
    base = nodes.Scan(data)

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

    report = nodes.Limit(
        nodes.Order(
            nodes.Selection(base, netflix_filter),
            netflix_score),
        50
    )
    print("REDSTREAK RESULT:")
    print("-" * 80)
    print(f"#: {'Title':39.39}|{'Genres':39.39}")

    print("-" * 80)
    for i, row in enumerate(report):
        print(f"{i:<2}: {row['title']:39.39}|{row['genres']:39.39}")


def main():
    with open(args.csv_path, newline='') as csvfile:
        run_query(csv.DictReader(csvfile))


if __name__ == '__main__':
    main()
