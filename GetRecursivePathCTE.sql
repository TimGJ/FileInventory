/*
** Given a file ID, recurse up the directory tree adding parent names as we go
*/ 

with recursive tree as (
	select id, name, parent from directory where id = 
	(select parent from file where id = 3790)
	union 
	select d.id, d.name, d.parent from directory d, tree t
	where t.parent = d.id
) select group_concat(name order by id separator '/') from tree ;
