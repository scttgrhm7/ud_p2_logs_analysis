#!/usr/bin/env python2.7

import psycopg2

# QUERIES
popular_articles = ("select title, count(*) as views from articles a "
                    "join log l on l.path ilike '%' || a.slug || '%' "
                    "group by 1 order by count(*) desc limit 3; ")

popular_writers = ("select name, sum(views) as views from authors a "
                   "join (select author, count(*) as views from articles a "
                   "join log l on l.path ilike '%' || a.slug || '%' "
                   "group by 1) as subq on subq.author = a.id group by 1 "
                   "order by sum(views) desc; ")

high_error_days = ("select day, round(sum(error)*100.0/count(error),2) "
                   "as errors from (select time::date as day, case when "
                   "status = '404 NOT FOUND' then 1 else 0 end as error, "
                   "status from log) as subq group by 1 having "
                   "sum(error)*1.0/count(error) > 0.01; ")


# CREATE DB CONNECTION AND CURSOR
db = psycopg2.connect(database="news")
c = db.cursor()


# PRINT OUT POPULAR ARTICLES
c.execute(popular_articles)
articles = c.fetchall()

print 'The most popular three articles of all time are:\n'

for article in articles:
    print ('Title: ' + '"' + str(article[0]) + '"' + ', ' + 'Views: ' +
    str(article[1]))

print('\n')


# PRINT OUT POPULAR AUTHORS
c.execute(popular_writers)
authors = c.fetchall()

print 'The most popular authors of all time are:\n'

for author in authors:
    print 'Author: ' + str(author[0]) + ', ' + 'Views: ' + str(author[1])

print('\n')


# PRINT OUT DAYS WITH >1% ERROR RATE
c.execute(high_error_days)
error_days = c.fetchall()

if len(error_days) > 1:
    print 'Errors exceeded 1% of all requests on these days:\n'
else:
    print 'Errors exceeded 1% of all requests on this day:\n'

for day in error_days:
    print 'Date: ' + str(day[0])

db.close()
