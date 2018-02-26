from serialize import readtups, write_fresh_table, TABLES


def rating_to_nt(_iter, recordtype):
    for row in _iter:

        yield recordtype.factory(
            userId=int(row["userId"]),
            movieId=int(row["movieId"]),
            rating=float(row["rating"]),
            timestamp=int(row["timestamp"]),
        )


def movie_to_nt(_iter, recordtype):
    for row in _iter:

        yield recordtype.factory(
            movieId=int(row["movieId"]),
            title=row["title"],
            genres=row["genres"],
        )


def main():
    csv_path = "1kmovies.csv"
    # csv_path = "../../ml-20m/ratings.csv"

    with open(csv_path, newline='') as csvfile:

        record_type = TABLES['Movie']
        with open(record_type.homefile, "wb") as newfile:

            write_fresh_table(
                newfile,
                movie_to_nt(readtups(csvfile), record_type)
            )


if __name__ == '__main__':
    main()
