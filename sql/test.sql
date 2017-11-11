.echo on

.print 'starting off test.sql'

PRAGMA foreign_keys=OFF;
PRAGMA journal_mode = WAL;

--BEGIN TRANSACTION;

DROP TABLE IF EXISTS sites;
CREATE TABLE "sites" (
    sti integer primary key,
    name text not null,
    address text,
    city text,
    state text,
    phone text,
    web text,
    postal text,
    country text,
    note text,
    usr integer, 
    ts timestamp DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS old_sites;
CREATE TABLE old_sites (
    sti integer primary key,
    name text not null
);

--COMMIT;

DROP TRIGGER IF EXISTS sites_delete;
CREATE TRIGGER ips_insert BEFORE DELETE ON sites
BEGIN
    insert into old_sites select sti, name from sites where sti = OLD.sti;
END;

.read test2.sql
