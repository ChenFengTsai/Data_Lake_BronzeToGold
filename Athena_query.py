import json

# Open the JSON file and load the data
with open('variables.json') as f:
    var = json.load(f)

# Extract the value of the "name" key
S3_BUCKET = var['data_lake_bucket']

AGG_SONG_BY_GENRE = f"""
    CREATE TABLE IF NOT EXISTS agg_song_by_genre
    WITH (
        format = 'Parquet',
        write_compression = 'SNAPPY',
        external_location = 's3://{S3_BUCKET}/tickit/gold/song_by_artist_genre/',
        partitioned_by = ARRAY [ 'genre'],
        bucketed_by = ARRAY [ 'bucket_genre' ],
        bucket_count = 1
    )

    SELECT cast(s.release_date as DATE) as rel_date,
        s.artist as artist,
        art.followers as followers,
        round(cast(art.popularity AS DECIMAL(8,2)), 2) as popularity,
        alb.name as album,
        art.genre as genre,
        s.energy as energy,
        s.speechiness as speechiness,
        s.tempo as tempo
    FROM refined_spotify_song AS s
        LEFT JOIN refined_spotify_artist AS art ON art.name = s.artist
        LEFT JOIN refined_spotify_album AS alb ON alb.name = s.album;
"""

AGG_SONG_BY_DATE = f"""
    CREATE TABLE IF NOT EXISTS agg_song_by_date
    WITH (
        format = 'Parquet',
        write_compression = 'SNAPPY',
        external_location = 's3://{S3_BUCKET}/spotify/gold/song_release_by_date/',
        partitioned_by = ARRAY [ 'year', 'month'],
        bucketed_by = ARRAY [ 'bucket_month' ],
        bucket_count = 1
    )

    SELECT cast(s.release_date as DATE) as rel_date,
        Month(cast(s.release_date as DATE)) as rel_month,
        Year(cast(s.release_date as DATE)) as rel_year,
        s.artist as artist,
        art.followers as followers,
        round(cast(art.popularity AS DECIMAL(8,2)), 2) as popularity,
        alb.name as album,
    FROM refined_spotify_song AS s
        LEFT JOIN refined_spotify_artist AS art ON art.name = s.artist
        LEFT JOIN refined_spotify_album AS alb ON alb.name = s.album;
"""

