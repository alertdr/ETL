QUERIES = {
    'filmwork': lambda ids: f'''
SELECT film_work.id,
       film_work.title,
       film_work.description,
       film_work.rating,
       film_work.creation_date,
       JSON_OBJECT_AGG(DISTINCT genre.name, genre.id)                             AS genre,
       JSON_OBJECT_AGG(DISTINCT person.full_name, person.id)
       FILTER (WHERE UPPER(person_film_work.role::text) LIKE UPPER('%actor%'))    AS actors,
       JSON_OBJECT_AGG(DISTINCT person.full_name, person.id)
       FILTER (WHERE UPPER(person_film_work.role::text) LIKE UPPER('%director%')) AS director,
       JSON_OBJECT_AGG(DISTINCT person.full_name, person.id)
       FILTER (WHERE UPPER(person_film_work.role::text) LIKE UPPER('%writer%'))   AS writers,
       ARRAY_AGG(DISTINCT person.modified)                                        AS person_time,
       ARRAY_AGG(DISTINCT genre.modified)                                         AS genres_time,
       film_work.modified
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
    'genre': lambda ids: f'''
SELECT genre.id,
       genre.name,
       genre.description,
       genre.modified
FROM genre
WHERE genre.id {f'in {ids}' if len(ids) > 1 else f"= '{ids[0]}'"}
GROUP BY genre.id
ORDER BY genre.modified;
''',
    'person': lambda ids: f'''
SELECT person.id,
       person.full_name,
       ARRAY_AGG(DISTINCT person_film_work.role)                                  AS roles,
       ARRAY_AGG(DISTINCT film_work.id)
       FILTER (WHERE UPPER(person_film_work.role::text) LIKE UPPER('%actor%'))    AS films_as_actor,
       ARRAY_AGG(DISTINCT film_work.id)
       FILTER (WHERE UPPER(person_film_work.role::text) LIKE UPPER('%director%')) AS films_as_director,
       ARRAY_AGG(DISTINCT film_work.id)
       FILTER (WHERE UPPER(person_film_work.role::text) LIKE UPPER('%writer%'))   AS films_as_writer,
       person.modified,
       ARRAY_AGG(DISTINCT film_work.modified)                                         AS filmwork_time
FROM person
         LEFT OUTER JOIN person_film_work
                         ON (person.id = person_film_work.person_id)
         LEFT OUTER JOIN film_work
                         ON (person_film_work.film_work_id = film_work.id)
WHERE person.id {f'in {ids}' if len(ids) > 1 else f"= '{ids[0]}'"}
GROUP BY person.id
ORDER BY person.modified;
    ''',
    'genre_ids': lambda genre_date: f'''
SELECT genre.id
FROM genre
WHERE {genre_date if genre_date else 'to_timestamp(0)'} < genre.modified
GROUP BY genre.id
ORDER BY genre.modified;
''',
    'person_ids': lambda person_date, filmwork_date: f'''
SELECT person.id
FROM person
         LEFT OUTER JOIN person_film_work
                         ON (person.id = person_film_work.person_id)
         LEFT OUTER JOIN film_work
                         ON (person_film_work.film_work_id = film_work.id)
WHERE
{person_date if person_date else 'to_timestamp(0)'} < person.modified OR
{filmwork_date if filmwork_date else 'to_timestamp(0)'} < film_work.modified
GROUP BY person.id
ORDER BY person.modified;
    ''',
    'filmwork_ids': lambda filmwork_date=None, genre_date=None, person_date=None: f'''
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
WHERE
{filmwork_date if filmwork_date else 'to_timestamp(0)'} < film_work.modified OR 
{genre_date if genre_date else 'to_timestamp(0)'} < genre.modified OR
{person_date if person_date else 'to_timestamp(0)'} < person.modified
GROUP BY film_work.id
ORDER BY film_work.modified;
    '''
}
