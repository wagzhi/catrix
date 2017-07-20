# catrix

example :
```sql
    CREATE KEYSPACE catrix WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;
    create table book (id int, name text, price double ,published_at timestamp ,primary key(id));
```
