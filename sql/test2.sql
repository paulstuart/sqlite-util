--BEGIN TRANSACTION;

DROP VIEW IF EXISTS summary;

CREATE VIEW summary as 
	select country, count(*) as total
	from sites
	group by country
	order by total desc
	;

insert into sites (name, country) values('San Francisco', 'USA');
insert into sites (name, country) values('New York', 'USA');
insert into sites (name, country) values('Dallas', 'USA');
insert into sites (name, country) values('Amsterdam', 'Netherlands');
insert into sites (name, country) values('Hong Kong', 'China');

.print "end of test 2"
