SELECT * FROM tweets_devoxx LIMIT 1;

SELECT jsonb_pretty(tweet) FROM tweets_devoxx LIMIT 1;

SELECT count(1) from tweets_devoxx;


-- nombre de tweet par jour
--SELECT
--  (tweet ->> 'createdAt') :: DATE AS day,
--  count(*)                       AS nb
--FROM tweets_devoxx
--GROUP BY day
--ORDER BY day desc;
SELECT  DATE_PART_MILLIS(createdAt, 'day') as day,
  count(*) AS nb
FROM tweets_devoxx
GROUP BY  DATE_PART_MILLIS(createdAt, 'day')
ORDER BY day desc;


CREATE OR REPLACE FUNCTION histo(nb BIGINT) RETURNS TEXT AS $$
BEGIN
  RETURN lpad('', (nb::INTEGER /20)+1, '▩');
END;
$$ LANGUAGE plpgsql;



-- nombre de tweet par jour de semaine
--SELECT count(*) as nb, date_part('dow', (tweet ->> 'createdAt')::date) as day_of_week
--FROM tweets_devoxx
--GROUP BY day_of_week
--ORDER BY nb DESC;
SELECT DATE_PART_MILLIS(createdAt, 'dow') as day_of_week,  count(*) AS nb
FROM tweets_devoxx
GROUP BY DATE_PART_MILLIS(createdAt, 'dow')
ORDER BY day_of_week desc;

-- qui tweet le plus
--SELECT count(1) as nb_tweet, handle
--FROM tweets_devoxx
--group by handle
--order by nb_tweet DESC
--limit 4;
SELECT count(1) as nb_tweet, handle
FROM tweets_devoxx
group by handle
order by nb_tweet DESC
limit 4;

-- le handle c'est sympa, mais on a pas plus d'info?
--SELECT count(1) as nb_tweet, handle, tweet -> 'user' ->> 'name' as name
--FROM tweets_devoxx
--group by handle, name
--order by nb_tweet DESC
--limit 10;
SELECT count(1) as nb_tweet, handle, `user.name` as name
FROM tweets_devoxx
group by handle, `user.name`
order by nb_tweet DESC
limit 10;

-- parmis les gens ayant tweeter sur devoxx qui sont ceux ayant le plus de follower
--SELECT
--  handle,
--  tweet -> 'user' ->> 'name'                             AS name,
--  max((tweet -> 'user' ->> 'followersCount') :: INTEGER) AS nbFollowers
--FROM tweets_devoxx
--GROUP BY handle, name
--ORDER BY nbFollowers DESC
--LIMIT 5;
SELECT
  handle,
  `user`.name    AS name,
  max(`user`.followersCount) AS nbFollowers
FROM tweets_devoxx
GROUP BY handle, `user`.name
ORDER BY nbFollowers DESC
LIMIT 5;


-- le tweet le plus retweeté
--SELECT
--  tweet -> 'retweetedStatus' ->> 'text'                           AS text,
--  tweet -> 'retweetedStatus' -> 'user' ->> 'name'                 AS name,
--  MAX((tweet -> 'retweetedStatus' ->> 'retweetCount') :: INTEGER) AS retweeted
--FROM tweets_devoxx
--WHERE tweet ? 'retweetedStatus'
--GROUP BY text, name ORDER BY retweeted DESC
--LIMIT 20;
SELECT
  retweetedStatus.text                           AS text,
  retweetedStatus.`user`.name                 AS name,
  MAX(retweetedStatus.retweetCount) AS retweeted
FROM tweets_devoxx
WHERE retweetedStatus IS NOT MISSING
GROUP BY retweetedStatus.text, retweetedStatus.`user`.name ORDER BY retweeted DESC
LIMIT 20;



-- les tweets fait proche du palais de congrès
--SELECT
--  handle, tweet -> 'user' ->> 'name', tweet ->> 'text'
--FROM tweets_devoxx
--WHERE tweet ? 'place'
--ORDER BY POINT((tweet -> 'place' -> 'boundingBoxCoordinates' -> 0 -> 0 ->> 'latitude') :: DECIMAL,
--               (tweet -> 'place' -> 'boundingBoxCoordinates' -> 0 -> 0 ->> 'longitude') :: DECIMAL) <->
--         '(48.877965, 2.281836)'
--LIMIT 5;

--CREATE INDEX TEST ON tweets_devoxx USING GIST (POINT((tweet -> 'place' -> 'boundingBoxCoordinates' -> 0 -> 0 ->> 'latitude') :: DECIMAL,
--(tweet -> 'place' -> 'boundingBoxCoordinates' -> 0 -> 0 ->> 'longitude') :: DECIMAL))
--  WHERE tweet ? 'place';


Needs a Spatial index


--SELECT
--  CASE WHEN tweet ? 'retweetedStatus' THEN (tweet -> 'retweetedStatus' ->> 'id')::BIGINT ELSE id END as id_origin,
--  CASE WHEN tweet ? 'retweetedStatus' THEN (tweet -> 'retweetedStatus' ->> 'text')::TEXT ELSE (tweet ->> 'text')::TEXT END as text_origin,
--  SUM((tweet -> 'user' ->> 'followersCount')::INTEGER) as followers,
--  count(1) as nb
--FROM tweets_devoxx
--GROUP BY id_origin, text_origin
--ORDER BY followers desc;
SELECT
  CASE WHEN retweetedStatus IS NOT NULL THEN retweetedStatus.id ELSE id END as id_origin,
  CASE WHEN retweetedStatus IS NOT NULL THEN retweetedStatus.text ELSE text END as text_origin,
  SUM(`user`.followersCount) as followers,
  count(1) as nb
FROM tweets_devoxx
GROUP BY CASE WHEN retweetedStatus IS NOT NULL THEN retweetedStatus.id ELSE id END,
  CASE WHEN retweetedStatus IS NOT NULL THEN retweetedStatus.text ELSE text END
ORDER BY followers desc;


--select jsonb_pretty(jsonb_agg(row_to_json(_))) as result
--FROM (
--  SELECT handle, count(1) AS nbtweet
--  FROM tweets_devoxx
--  GROUP BY handle
--)_;
SELECT handle, count(1) AS nbtweet
  FROM tweets_devoxx
  GROUP BY handle


--select jsonb_pretty(jsonb_object_agg(handle, nbtweet))
--FROM (
--       SELECT handle, count(1) AS nbtweet
--       FROM tweets_devoxx
--       GROUP BY handle
--     )_;
SELECT handle, count(1) AS nbtweet
  FROM tweets_devoxx
  GROUP BY handle



--CREATE TABLE tweets_devoxx
--(
--  id BIGINT PRIMARY KEY NOT NULL,
--  handle TEXT NOT NULL,
--  tweet JSONB NOT NULL
--);



--CREATE TABLE tweets_devoxx_big
--(
--  id BIGINT PRIMARY KEY NOT NULL,
--  handle TEXT NOT NULL,
--  tweet JSONB NOT NULL
--);


--select tweet ->> 'createdAt',
--  id, handle,
--  tweet -> 'user' ->> 'name',
--  tweet ->> 'text'
--FROM tweets_devoxx_big
--ORDER BY tweet ->> 'createdAt' asc LIMIT 5;
select createdAt, Meta().id, handle, `user`.name,text
FROM tweets_devoxx
ORDER BY createdAt asc LIMIT 5;

--CREATE INDEX created_at_idx ON tweets_devoxx_big ((tweet ->> 'createdAt'));
CREATE SECONDARY INDEX created_at_idx ON tweets_devoxx (createdAt);

--select handle, tweet, ((tweet ->> 'retweetCount')::INTEGER) rtCount from tweets_devoxx_big
--ORDER BY rtCount desc limit 5;
select * from tweets_devoxx
ORDER BY retweetCount desc limit 5;

--CREATE INDEX retweet_count_idx ON tweets_devoxx_big (((tweet ->> 'retweetCount')::INTEGER));
CREATE INDEX retweet_count_idx ON tweets_devoxx (retweetCount);


--SELECT handle, count(1) as nb_mentions FROM tweets_devoxx_big
--WHERE tweet -> 'userMentionEntities' @> '[{"screenName": "ybonnel"}]'
--group by handle order by nb_mentions desc;
SELECT handle, count(1) as nb_mentions FROM tweets_devoxx
WHERE any mention in userMentionEntities satisfies mention.screenName = 'voxxed' END
group by handle order by nb_mentions desc;

--CREATE INDEX user_mention_idx ON tweets_devoxx_big USING gin ((tweet -> 'userMentionEntities'));
CREATE INDEX user_mention_idx ON tweets_devoxx USING gin (userMentionEntities);
