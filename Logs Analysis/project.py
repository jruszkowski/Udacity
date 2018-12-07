#!usr/bin/env python3

import psycopg2

DBNAME = "news"

question_1 = "\
        select b.title, count(a.path) as downloads \
        from log a, articles b \
        where substring(a.path,10,255) = b.slug \
        and a.path!='/' \
        and a.status = '200 OK' \
        group by 1 \
        order by 2 \
        desc limit 3"

question_2 = "\
        select c.name, count(a.path) as downloads \
        from log a, articles b, authors c \
        where substring(a.path,10,255) = b.slug \
                and b.author = c.id \
        and a.path!='/' \
        and a.status = '200 OK' \
        group by 1 \
        order by 2 \
        desc limit 3"

question_3 = "\
        select \
        a.date_notime,\
        b.bad::int::float / a.total::int::float * 100 as errors\
        from\
        (select time::timestamp::date as date_notime,count(*) as total\
            from log group by 1) a,\
        (select time::timestamp::date as date_notime,count(*) as bad\
            from log where status = '404 NOT FOUND' group by 1) b\
        where a.date_notime = b.date_notime\
        and (b.bad::int::float / a.total::int::float) > .01\
        "

def results(q):
    db = psycopg2.connect(database=DBNAME)
    c = db.cursor()
    c.execute(q) 
    posts = c.fetchall()
    db.close()
    return posts


def pprint(l, t=' views'):
    if t == '% errors':
        for x in l:
            print('%s, %.2f%s' % (x[0], x[1], t))
    else:
        for x in l:
            print('%s, %d%s' % (x[0], x[1], t))


if __name__ == '__main__': 
    print('Question #1')
    pprint(results(question_1))
    print('Question #2')
    pprint(results(question_2))
    print('Question #3')
    pprint(results(question_3), '% errors')
