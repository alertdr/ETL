QUERIES = {
    'filmwork': lambda ids: f'''
SELECT film_work.id,
       film_work.title,
       film_work.description,
       film_work.rating,
       ARRAY_AGG(DISTINCT genre.name)                                             AS genre,
       JSON_OBJECT_AGG(DISTINCT person.full_name, person.id)
       FILTER (WHERE UPPER(person_film_work.role::text) LIKE UPPER('%actor%'))    AS actors,
       ARRAY_AGG(DISTINCT person.full_name)
       FILTER (WHERE UPPER(person_film_work.role::text) LIKE UPPER('%director%')) AS director,
       JSON_OBJECT_AGG(DISTINCT person.full_name, person.id)
       FILTER (WHERE UPPER(person_film_work.role::text) LIKE UPPER('%writer%'))   AS writers,
       ARRAY_AGG(DISTINCT person.modified)                                        AS person_time,
       ARRAY_AGG(DISTINCT genre.modified)                                         AS genres_time
FROM film_work
         LEFT OUTER JOIN genre_film_work
                         ON (film_work.id = genre_film_work.film_work_id)
         LEFT OUTER JOIN genre
                         ON (genre_film_work.genre_id = genre.id)
         LEFT OUTER JOIN person_film_work
                         ON (film_work.id = person_film_work.film_work_id)
         LEFT OUTER JOIN person
                         ON (person_film_work.person_id = person.id)
WHERE film_work.id {f'in {ids}' if len(ids) > 1 else f"= '{ids[0]}'"}
GROUP BY film_work.id
ORDER BY film_work.modified;
''',
    'get_ids': lambda filmwork_date=None, genre_date=None, person_date=None: f'''
SELECT film_work.id
FROM film_work
         LEFT OUTER JOIN genre_film_work
                         ON (film_work.id = genre_film_work.film_work_id)
         LEFT OUTER JOIN genre
                         ON (genre_film_work.genre_id = genre.id)
         LEFT OUTER JOIN person_film_work
                         ON (film_work.id = person_film_work.film_work_id)
         LEFT OUTER JOIN person
                         ON (person_film_work.person_id = person.id)
WHERE  {filmwork_date if filmwork_date else 'to_timestamp(0)'} < film_work.modified OR 
{genre_date if genre_date else 'to_timestamp(0)'} < genre.modified OR
{person_date if person_date else 'to_timestamp(0)'} < person.modified
GROUP BY film_work.id
ORDER BY film_work.modified;
    '''
}
