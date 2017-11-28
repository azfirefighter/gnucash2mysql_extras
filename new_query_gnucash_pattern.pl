#!/usr/bin/perl

use DBI;
use DBD::mysql;
use Date::Manip;
use Data::Dumper;
use strict;
use Getopt::Long;
my ($start_dt, $end_dt);

# Some date formats for Date::Manip;

my $db_fmt  = "%Y-%m-%d";
my $http_date_fmt = "%g";


my %opt = ('start_dt' => '2016-06-01',
	   'end_dt' => '2016-12-01',
	   'pattern' => '"Book%"',
	   'increment' => "+1 month",
	   );
	   
GetOptions(\%opt,"start_dt:s","end_dt:s","pattern:s","increment:s");

print Dumper(\%opt);


my $query=<<'QUERY';
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
    where posted >= ?
    and posted <= ?
    and (a.name like ? or aa.name like ?)
    group by date_format(posted, '%Y-%m'), a.name
) as x
group by name
order by name;
QUERY
;



# parse up the start_dt and end_dt

my $start_dt_parsed = ParseDate($opt{'start_dt'});
my $end_dt_parsed = ParseDate($opt{'end_dt'});
my $pattern =  $opt{"pattern"};
print "Pattern is $pattern\n";
# loop from start_dt to end_dt in increments of 1 month
my $left_dt = $start_dt_parsed;
my $right_dt = DateCalc($start_dt_parsed, $opt{'increment'});
my $interval_hr;

my $grand_total;
while ($right_dt <= $end_dt_parsed) {
  $left_dt = DateCalc($left_dt, $opt{'increment'});
  $right_dt = DateCalc($right_dt, $opt{'increment'});
  my $left_dt_dbfmt = UnixDate($left_dt,$db_fmt);
  my $right_dt_dbfmt = UnixDate($right_dt,$db_fmt);
  
  print "From " . $left_dt_dbfmt . " to " . $right_dt_dbfmt . "\t";
#  $interval_hr = do_query($left_dt_dbfmt,$right_dt_dbfmt,$query);
  $interval_hr = do_query($left_dt_dbfmt,$right_dt_dbfmt,$pattern,$query);
  my $total = 0;
  foreach my $val (values(%{$interval_hr})) {
    $total += $val;
  }
  print "Total is $total\n";
  $grand_total += $total;
}  
print "\nGrand Total for range: $grand_total\n\n";





sub do_query {
  my $left_dt = shift;
  my $right_dt = shift;
  my $pattern = shift;
  my $query = shift;

  my $dsn = 'DBI:mysql:gnucash_db:localhost';
  my $db_user_name='root';
  my $db_password='root';
  
  my $dbh = DBI->connect($dsn,$db_user_name,$db_password) or die "Failed:";
  my $sth = $dbh->prepare($query) or die "Failed: $dbh->errstr";
#  $sth->bind_param(1,$left_dt);
#  $sth->bind_param(2,$right_dt);
#  $sth->bind_param(3,$pattern);
  $sth->execute($left_dt,$right_dt,'%'.$pattern.'%','%'.$pattern.'%')  or die "Failed: $sth->errstr";
#  $sth->execute($left_dt,$right_dt) or die "Failed: $sth->errstr";


#  print "in do_query, left_dt is $left_dt and right_dt is $right_dt\n";
# For some reason, we have to explicitly call bind param, executing the statement
# using the hash value returns nothing, while passing them in as variables works.
# The explicit bind works with the hash. Go figure.

# $sth->bind_param(1,$opt{'start_dt'});
# $sth->bind_param(2,$opt{'end_dt'});

  


  my $total_hr;
  while (my $hash_ref = $sth->fetchrow_hashref) {
#    print Dumper($hash_ref);
    $total_hr->{$hash_ref->{"name"}} = $hash_ref->{"Total  amount"};
  }

  return $total_hr;
#  print Dumper($total_hr);

#my $total = 0;
#foreach my $val (values(%{$total_hr})) {
#  $total += $val;
#}
#print "Total is $total\n";

}
