select @num_months := count(distinct date_format(posted, '%Y-%m'))
    from transaction
    where posted >= '2005-01-01';


select cast(sum(amount)  as decimal(8,2)) as 'Total  amount',
    concat(coalesce(grandparent_name, ''),
        if(grandparent_name is null, '', ' > '),
        coalesce(parent_name, ''),
        if(parent_name is null, '', ' > '),
        name) as name
from (
    select date_format(posted, '%Y-%m') as month,
        a.name,
        aa.name as parent_name,
        aaa.name as grandparent_name,
        sum(amount) as amount
    from transaction as t
        inner join split as s on s.transaction = t.id
        inner join (
            select id, name, parent from account
            where type='EXPENSE'
        ) as a on a.id = s.account
        left outer join account as aa on aa.id = a.parent
        left outer join account as aaa on aaa.id = aa.parent
    where posted >= '2016-02-01' 
    and posted <= '2016-11-01'
    and aa.name like "Book%"
    group by date_format(posted, '%Y-%m'), a.name
) as x
group by name
order by name;
