# catrix

example :
```sql
    create keyspace catrix WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;
    create table book (id int, name text, price double ,published_at timestamp ,primary key(id));
    create table page(domain text,url text,content text,created_at TIMESTAMP, PRIMARY KEY (domain,created_at,url));
    create index page_url on page (url);
```
