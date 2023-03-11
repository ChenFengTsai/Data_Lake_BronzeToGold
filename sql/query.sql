SELECT
    year(rel_date) AS rel_year,
    month(rel_date) AS rel_month,
    round(sum(enery), 2) AS energy,
    round(sum(speechiness), 2) AS speechiness,
FROM agg_spotify_song_by_genre
GROUP BY genre
ORDER BY year(caldate), month(caldate);
