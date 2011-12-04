package test::Dongry::Database::execute;
use strict;
use warnings;
use Path::Class;
use lib file (__FILE__)->dir->parent->subdir ('lib')->stringify;
use Test::Dongry;
use base qw(Test::Class);
use Dongry::Database;

sub _execute_create_table_no_return : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'createtable';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});

  $db->execute
      ('create table table1 (id int unsigned not null primary key)');
  
  my $db2 = Dongry::Database->new
      (sources => {default => {dsn => $dsn}});
  lives_ok { $db2->execute ('select * from table1') };
} # _execute_create_table

sub _execute_create_table_no_return_no_master : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'createtable';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 1}});

  dies_ok {
    $db->execute
        ('create table table1 (id int unsigned not null primary key)');
  };

  my $db2 = Dongry::Database->new
      (sources => {default => {dsn => $dsn}});
  dies_ok { $db2->execute ('select * from table1') };
} # _execute_create_table_no_master

sub _execute_create_table_not_writable : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'createtable';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 0}});

  dies_ok {
    $db->execute
        ('create table table1 (id int unsigned not null primary key)');
  };

  my $db2 = Dongry::Database->new
      (sources => {default => {dsn => $dsn}});
  dies_ok { $db2->execute ('select * from table1') };
} # _execute_create_table_not_writable

sub _execute_insert_no_return : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'createtable';
  my $db0 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db0->execute
      ('create table table1 (id int unsigned not null primary key)');

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});

  $db->execute ('insert into table1 (id) values (5)');
  
  my $db2 = Dongry::Database->new
      (sources => {default => {dsn => $dsn}});
  is $db2->execute ('select * from table1')->row_count, 1;
} # _execute_insert_no_return

sub _execute_insert_no_return_2 : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'createtable';
  my $db0 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db0->execute
      ('create table table1 (id int unsigned not null primary key)');

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});

  $db->execute ('INSERT INTO table1 (id) values (5)');
  
  my $db2 = Dongry::Database->new
      (sources => {default => {dsn => $dsn}});
  is $db2->execute ('select * from table1')->row_count, 1;
} # _execute_insert_no_return_2

sub _execute_insert_no_return_not_writable : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'createtable';
  my $db0 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db0->execute
      ('create table table1 (id int unsigned not null primary key)');

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 0}});
  dies_ok {
    $db->execute ('insert into table1 (id) values (5)');
  };

  my $db2 = Dongry::Database->new
      (sources => {default => {dsn => $dsn}});
  is $db2->execute ('select * from table1')->row_count, 0;
} # _execute_insert_no_return

sub _execute_select_no_return : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'testtable';
  my $db0 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db0->execute
      ('create table table1 (id int unsigned not null primary key)');

  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn}});

  lives_ok {
    $db->execute ('select * from table1');
  };
} # _execute_select_no_return

sub _execute_select_no_return_2 : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'testtable';
  my $db0 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db0->execute
      ('create table table1 (id int unsigned not null primary key)');

  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn}});

  lives_ok {
    $db->execute ('SELECT * FROM table1');
  };
} # _execute_select_no_return_2

sub _execute_select_no_return_no_default : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'testtable';
  my $db0 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db0->execute
      ('create table table1 (id int unsigned not null primary key)');

  my $db = Dongry::Database->new
      (sources => {test => {dsn => $dsn}});

  dies_ok {
    $db->execute ('select * from table1');
  };
} # _execute_select_no_return_no_default

sub _execute_show_tables_no_return : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'testtable';
  my $db0 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db0->execute
      ('create table table1 (id int unsigned not null primary key)');

  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn}});
  lives_ok {
    $db->execute ('show tables');
  };
} # _execute_show_tables_no_return

sub _execute_explain_no_return : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'testtable';
  my $db0 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db0->execute
      ('create table table1 (id int unsigned not null primary key)');

  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn}});
  lives_ok {
    $db->execute ('explain select * from table1');
  };
} # _execute_explain_no_return

sub _execute_select_return_no_rows_each : Test(11) {
  reset_db_set;
  my $dsn = test_dsn 'testtable';
  my $db0 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db0->execute
      ('create table table1 (id int unsigned not null primary key)');

  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn}});

  my $result = $db->execute ('select * from table1');
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 0;
  is $result->table_name, undef;
  my $invoked = 0;
  $result->each (sub { $invoked++ });
  is $invoked, 0;
  dies_ok { $result->each (sub { $invoked++ }) };
  dies_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 0;
  dies_ok { $result->all };
  dies_ok { $result->all_as_rows };
  dies_ok { $result->first };
  dies_ok { $result->first_as_row };
} # _execute_select_return_no_rows_each

sub _execute_select_return_no_rows_each_as_row : Test(12) {
  reset_db_set;
  my $dsn = test_dsn 'testtable';
  my $db0 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db0->execute
      ('create table table1 (id int unsigned not null primary key)');

  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn}});

  my $result = $db->execute ('select * from table1');
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 0;
  is $result->table_name, undef;
  my $invoked = 0;
  dies_ok { $result->each_as_row (sub { $invoked++ }) };
  dies_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 0;
  lives_ok { $result->each (sub { $invoked++ }) };
  is $invoked, 0;
  dies_ok { $result->all };
  dies_ok { $result->all_as_rows };
  dies_ok { $result->first };
  dies_ok { $result->first_as_row };
} # _execute_select_return_no_rows_each_as_row

sub _execute_select_return_no_rows_all : Test(11) {
  reset_db_set;
  my $dsn = test_dsn 'testtable';
  my $db0 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db0->execute
      ('create table table1 (id int unsigned not null primary key)');

  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn}});

  my $result = $db->execute ('select * from table1');
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 0;
  is $result->table_name, undef;
  my $all = $result->all;
  isa_list_n_ok $all, 0;
  my $invoked = 0;
  dies_ok { $result->each (sub { $invoked++ }) };
  dies_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 0;
  dies_ok { $result->all };
  dies_ok { $result->all_as_rows };
  dies_ok { $result->first };
  dies_ok { $result->first_as_row };
} # _execute_select_return_no_rows_all

sub _execute_select_return_no_rows_all_as_rows : Test(12) {
  reset_db_set;
  my $dsn = test_dsn 'testtable';
  my $db0 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db0->execute
      ('create table table1 (id int unsigned not null primary key)');

  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn}});

  my $result = $db->execute ('select * from table1');
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 0;
  is $result->table_name, undef;
  dies_ok { $result->all_as_rows };
  my $all = $result->all;
  isa_list_n_ok $all, 0;
  my $invoked = 0;
  dies_ok { $result->each (sub { $invoked++ }) };
  dies_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 0;
  dies_ok { $result->all_as_rows };
  dies_ok { $result->all };
  dies_ok { $result->first };
  dies_ok { $result->first_as_row };
} # _execute_select_return_no_rows_all_as_rows

sub _execute_select_return_no_rows_first : Test(11) {
  reset_db_set;
  my $dsn = test_dsn 'testtable';
  my $db0 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db0->execute
      ('create table table1 (id int unsigned not null primary key)');

  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn}});

  my $result = $db->execute ('select * from table1');
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 0;
  is $result->table_name, undef;
  my $first = $result->first;
  is $first, undef;
  dies_ok { $result->first };
  dies_ok { $result->first_as_row };
  my $invoked = 0;
  dies_ok { $result->each (sub { $invoked++ }) };
  dies_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 0;
  dies_ok { $result->all };
  dies_ok { $result->all_as_rows };
} # _execute_select_return_no_rows_first

sub _execute_select_return_no_rows_first_as_row : Test(11) {
  reset_db_set;
  my $dsn = test_dsn 'testtable';
  my $db0 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db0->execute
      ('create table table1 (id int unsigned not null primary key)');

  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn}});

  my $result = $db->execute ('select * from table1');
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 0;
  is $result->table_name, undef;
  dies_ok { $result->first_as_row };
  my $first = $result->first;
  is $first, undef;
  dies_ok { $result->first };
  my $invoked = 0;
  dies_ok { $result->each (sub { $invoked++ }) };
  dies_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 0;
  dies_ok { $result->all };
  dies_ok { $result->all_as_rows };
} # _execute_select_return_no_rows_first_as_row

sub _execute_select_return_a_row_each : Test(12) {
  reset_db_set;
  my $dsn = test_dsn 'testtable';
  my $db0 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db0->execute
      ('create table table1 (id int unsigned not null primary key,
                             value text)');
  $db0->execute
      ('insert into table1 (id) values (1253)');

  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn}});

  my $result = $db->execute ('select * from table1');
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 1;
  is $result->table_name, undef;
  my $invoked = 0;
  my @value;
  $result->each (sub { push @value, $_; $invoked++ });
  is $invoked, 1;
  eq_or_diff \@value, [{id => 1253, value => undef}];
  dies_ok { $result->each (sub { $invoked++ }) };
  dies_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 1;
  dies_ok { $result->all };
  dies_ok { $result->all_as_rows };
  dies_ok { $result->first };
  dies_ok { $result->first_as_row };
} # _execute_select_return_a_row_each

sub _execute_select_return_a_row_each_as_row : Test(12) {
  reset_db_set;
  my $dsn = test_dsn 'testtable';
  my $db0 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db0->execute
      ('create table table1 (id int unsigned not null primary key,
                             value text)');
  $db0->execute
      ('insert into table1 (id) values (1253)');

  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn}});

  my $result = $db->execute ('select * from table1');
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 1;
  is $result->table_name, undef;
  my $invoked = 0;
  dies_ok { $result->each_as_row (sub { $invoked++ }) };
  dies_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 0;
  lives_ok { $result->each (sub { $invoked++ }) };
  is $invoked, 1;
  dies_ok { $result->all };
  dies_ok { $result->all_as_rows };
  dies_ok { $result->first };
  dies_ok { $result->first_as_row };
} # _execute_select_return_a_row_each_as_row

sub _execute_select_return_a_row_all : Test(12) {
  reset_db_set;
  my $dsn = test_dsn 'testtable';
  my $db0 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db0->execute
      ('create table table1 (id int unsigned not null primary key,
                             value text)');
  $db0->execute
      ('insert into table1 (id) values (1253)');

  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn}});

  my $result = $db->execute ('select * from table1');
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 1;
  is $result->table_name, undef;
  my $all = $result->all;
  isa_list_n_ok $all, 1;
  eq_or_diff $all->to_a, [{id => 1253, value => undef}];
  my $invoked = 0;
  dies_ok { $result->each (sub { $invoked++ }) };
  dies_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 0;
  dies_ok { $result->all };
  dies_ok { $result->all_as_rows };
  dies_ok { $result->first };
  dies_ok { $result->first_as_row };
} # _execute_select_return_a_row_all

sub _execute_select_return_a_row_all_as_rows : Test(12) {
  reset_db_set;
  my $dsn = test_dsn 'testtable';
  my $db0 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db0->execute
      ('create table table1 (id int unsigned not null primary key,
                             value text)');
  $db0->execute
      ('insert into table1 (id) values (1253)');

  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn}});

  my $result = $db->execute ('select * from table1');
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 1;
  is $result->table_name, undef;
  dies_ok { $result->all_as_rows };
  my $all = $result->all;
  isa_list_n_ok $all, 1;
  my $invoked = 0;
  dies_ok { $result->each (sub { $invoked++ }) };
  dies_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 0;
  dies_ok { $result->all_as_rows };
  dies_ok { $result->all };
  dies_ok { $result->first };
  dies_ok { $result->first_as_row };
} # _execute_select_return_a_row_all_as_rows

sub _execute_select_return_a_row_first : Test(11) {
  reset_db_set;
  my $dsn = test_dsn 'testtable';
  my $db0 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db0->execute
      ('create table table1 (id int unsigned not null primary key,
                             value text)');
  $db0->execute
      ('insert into table1 (id) values (1253)');

  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn}});

  my $result = $db->execute ('select * from table1');
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 1;
  is $result->table_name, undef;
  my $first = $result->first;
  eq_or_diff $first, {id => 1253, value => undef};
  dies_ok { $result->first };
  dies_ok { $result->first_as_row };
  my $invoked = 0;
  dies_ok { $result->each (sub { $invoked++ }) };
  dies_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 0;
  dies_ok { $result->all };
  dies_ok { $result->all_as_rows };
} # _execute_select_return_a_row_first

sub _execute_select_return_a_row_first_as_row : Test(11) {
  reset_db_set;
  my $dsn = test_dsn 'testtable';
  my $db0 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db0->execute
      ('create table table1 (id int unsigned not null primary key,
                             value text)');
  $db0->execute
      ('insert into table1 (id) values (1253)');

  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn}});

  my $result = $db->execute ('select * from table1');
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 1;
  is $result->table_name, undef;
  dies_ok { $result->first_as_row };
  my $first = $result->first;
  eq_or_diff $first, {id => 1253, value => undef};
  dies_ok { $result->first };
  my $invoked = 0;
  dies_ok { $result->each (sub { $invoked++ }) };
  dies_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 0;
  dies_ok { $result->all };
  dies_ok { $result->all_as_rows };
} # _execute_select_return_a_row_first_as_row

sub _execute_select_return_multiple_rows_each : Test(12) {
  reset_db_set;
  my $dsn = test_dsn 'testtable';
  my $db0 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db0->execute
      ('create table table1 (id int unsigned not null primary key,
                             value text)');
  $db0->execute
      ('insert into table1 (id, value)
        values (1253, NULL), (3113, "hoge"), (10, 333)');

  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn}});

  my $result = $db->execute ('select * from table1 order by id asc');
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 3;
  is $result->table_name, undef;
  my $invoked = 0;
  my @value;
  $result->each (sub { push @value, $_; $invoked++ });
  is $invoked, 3;
  eq_or_diff \@value, [{id => 10, value => 333},
                       {id => 1253, value => undef},
                       {id => 3113, value => 'hoge'}];
  dies_ok { $result->each (sub { $invoked++ }) };
  dies_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 3;
  dies_ok { $result->all };
  dies_ok { $result->all_as_rows };
  dies_ok { $result->first };
  dies_ok { $result->first_as_row };
} # _execute_select_return_multiple_rows_each

sub _execute_select_return_multiple_rows_each_as_row : Test(12) {
  reset_db_set;
  my $dsn = test_dsn 'testtable';
  my $db0 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db0->execute
      ('create table table1 (id int unsigned not null primary key,
                             value text)');
  $db0->execute
      ('insert into table1 (id, value)
        values (1253, NULL), (3113, "hoge"), (10, 333)');

  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn}});

  my $result = $db->execute ('select * from table1');
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 3;
  is $result->table_name, undef;
  my $invoked = 0;
  dies_ok { $result->each_as_row (sub { $invoked++ }) };
  dies_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 0;
  lives_ok { $result->each (sub { $invoked++ }) };
  is $invoked, 3;
  dies_ok { $result->all };
  dies_ok { $result->all_as_rows };
  dies_ok { $result->first };
  dies_ok { $result->first_as_row };
} # _execute_select_return_multiple_rows_each_as_row

sub _execute_select_return_multiple_rows_all : Test(12) {
  reset_db_set;
  my $dsn = test_dsn 'testtable';
  my $db0 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db0->execute
      ('create table table1 (id int unsigned not null primary key,
                             value text)');
  $db0->execute
      ('insert into table1 (id, value)
        values (1253, NULL), (3113, "hoge"), (10, 333)');

  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn}});

  my $result = $db->execute ('select * from table1 order by id asc');
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 3;
  is $result->table_name, undef;
  my $all = $result->all;
  isa_list_n_ok $all, 3;
  eq_or_diff $all->to_a, [{id => 10, value => 333},
                       {id => 1253, value => undef},
                       {id => 3113, value => 'hoge'}];
  my $invoked = 0;
  dies_ok { $result->each (sub { $invoked++ }) };
  dies_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 0;
  dies_ok { $result->all };
  dies_ok { $result->all_as_rows };
  dies_ok { $result->first };
  dies_ok { $result->first_as_row };
} # _execute_select_return_multiple_rows_all

sub _execute_select_return_multiple_rows_all_as_rows : Test(12) {
  reset_db_set;
  my $dsn = test_dsn 'testtable';
  my $db0 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db0->execute
      ('create table table1 (id int unsigned not null primary key,
                             value text)');
  $db0->execute
      ('insert into table1 (id, value)
        values (1253, NULL), (3113, "hoge"), (10, 333)');

  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn}});

  my $result = $db->execute ('select * from table1');
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 3;
  is $result->table_name, undef;
  dies_ok { $result->all_as_rows };
  my $all = $result->all;
  isa_list_n_ok $all, 3;
  my $invoked = 0;
  dies_ok { $result->each (sub { $invoked++ }) };
  dies_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 0;
  dies_ok { $result->all_as_rows };
  dies_ok { $result->all };
  dies_ok { $result->first };
  dies_ok { $result->first_as_row };
} # _execute_select_return_multiple_rows_all_as_rows

sub _execute_select_return_multiple_rows_first : Test(11) {
  reset_db_set;
  my $dsn = test_dsn 'testtable';
  my $db0 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db0->execute
      ('create table table1 (id int unsigned not null primary key,
                             value text)');
  $db0->execute
      ('insert into table1 (id, value)
        values (1253, NULL), (3113, "hoge"), (10, 333)');

  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn}});

  my $result = $db->execute ('select * from table1 order by id asc');
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 3;
  is $result->table_name, undef;
  my $first = $result->first;
  eq_or_diff $first, {id => 10, value => '333'};
  dies_ok { $result->first };
  dies_ok { $result->first_as_row };
  my $invoked = 0;
  dies_ok { $result->each (sub { $invoked++ }) };
  dies_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 0;
  dies_ok { $result->all };
  dies_ok { $result->all_as_rows };
} # _execute_select_return_multiple_rows_first

sub _execute_select_return_multiple_rows_first_as_row : Test(11) {
  reset_db_set;
  my $dsn = test_dsn 'testtable';
  my $db0 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db0->execute
      ('create table table1 (id int unsigned not null primary key,
                             value text)');
  $db0->execute
      ('insert into table1 (id, value)
        values (1253, NULL), (3113, "hoge"), (10, 333)');

  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn}});

  my $result = $db->execute ('select * from table1 order by id asc');
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 3;
  is $result->table_name, undef;
  dies_ok { $result->first_as_row };
  my $first = $result->first;
  eq_or_diff $first, {id => 10, value => 333};
  dies_ok { $result->first };
  my $invoked = 0;
  dies_ok { $result->each (sub { $invoked++ }) };
  dies_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 0;
  dies_ok { $result->all };
  dies_ok { $result->all_as_rows };
} # _execute_select_return_multiple_rows_first_as_row

sub _execute_insert_return_a_row_each : Test(10) {
  reset_db_set;
  my $dsn = test_dsn 'testtable';
  my $db0 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db0->execute
      ('create table table1 (id int unsigned not null primary key,
                             value text)');

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});

  my $result = $db->execute
      ('insert into table1 (id, value) values (1253, NULL)');
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 1;
  is $result->table_name, undef;
  my $invoked = 0;
  dies_ok { $result->each (sub { $invoked++ }) };
  dies_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 0;
  dies_ok { $result->all };
  dies_ok { $result->all_as_rows };
  dies_ok { $result->first };
  dies_ok { $result->first_as_row };
} # _execute_insert_return_a_row_each

sub _execute_insert_return_a_row_all : Test(10) {
  reset_db_set;
  my $dsn = test_dsn 'testtable';
  my $db0 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db0->execute
      ('create table table1 (id int unsigned not null primary key,
                             value text)');

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});

  my $result = $db->execute
      ('insert into table1 (id, value) values (1253, NULL)');
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 1;
  is $result->table_name, undef;
  dies_ok { $result->all };
  dies_ok { $result->all_as_rows };
  dies_ok { $result->first };
  dies_ok { $result->first_as_row };
  my $invoked = 0;
  dies_ok { $result->each (sub { $invoked++ }) };
  dies_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 0;
} # _execute_insert_return_a_row_all

sub _execute_insert_return_a_row_first : Test(10) {
  reset_db_set;
  my $dsn = test_dsn 'testtable';
  my $db0 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db0->execute
      ('create table table1 (id int unsigned not null primary key,
                             value text)');

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});

  my $result = $db->execute
      ('insert into table1 (id, value) values (1253, NULL)');
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 1;
  is $result->table_name, undef;
  dies_ok { $result->first };
  dies_ok { $result->first_as_row };
  my $invoked = 0;
  dies_ok { $result->each (sub { $invoked++ }) };
  dies_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 0;
  dies_ok { $result->all };
  dies_ok { $result->all_as_rows };
} # _execute_insert_return_a_row_first

sub _execute_insert_return_multiple_row_all : Test(10) {
  reset_db_set;
  my $dsn = test_dsn 'testtable';
  my $db0 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db0->execute
      ('create table table1 (id int unsigned not null primary key,
                             value text)');

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});

  my $result = $db->execute
      ('insert into table1 (id, value)
        values (1253, NULL), (1424, "tex"), (10, "gseg aea")');
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 3;
  is $result->table_name, undef;
  dies_ok { $result->all };
  dies_ok { $result->all_as_rows };
  dies_ok { $result->first };
  dies_ok { $result->first_as_row };
  my $invoked = 0;
  dies_ok { $result->each (sub { $invoked++ }) };
  dies_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 0;
} # _execute_insert_return_multiple_row_all

sub _execute_update_return_no_row_all : Test(10) {
  reset_db_set;
  my $dsn = test_dsn 'testtable';
  my $db0 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db0->execute
      ('create table table1 (id int unsigned not null primary key,
                             value text)');
  $db0->execute
      ('insert into table1 (id, value)
        values (1253, NULL), (1424, "tex"), (10, "gseg aea")');

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});

  my $result = $db->execute
      ('update table1 set id = 20 where value = "not found"');
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 0;
  is $result->table_name, undef;
  dies_ok { $result->all };
  dies_ok { $result->all_as_rows };
  dies_ok { $result->first };
  dies_ok { $result->first_as_row };
  my $invoked = 0;
  dies_ok { $result->each (sub { $invoked++ }) };
  dies_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 0;
} # _execute_update_return_no_row_all

sub _execute_update_return_a_row_all : Test(10) {
  reset_db_set;
  my $dsn = test_dsn 'testtable';
  my $db0 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db0->execute
      ('create table table1 (id int unsigned not null primary key,
                             value text)');
  $db0->execute
      ('insert into table1 (id, value)
        values (1253, NULL), (1424, "tex"), (10, "gseg aea")');

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});

  my $result = $db->execute
      ('update table1 set id = 20 where value = "tex"');
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 1;
  is $result->table_name, undef;
  dies_ok { $result->all };
  dies_ok { $result->all_as_rows };
  dies_ok { $result->first };
  dies_ok { $result->first_as_row };
  my $invoked = 0;
  dies_ok { $result->each (sub { $invoked++ }) };
  dies_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 0;
} # _execute_update_return_a_row_all

sub _execute_update_return_multiple_rows_all : Test(10) {
  reset_db_set;
  my $dsn = test_dsn 'testtable';
  my $db0 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db0->execute
      ('create table table1 (id int unsigned not null primary key,
                             value text)');
  $db0->execute
      ('insert into table1 (id, value)
        values (1253, NULL), (1424, "tex"), (10, "gseg aea")');

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});

  my $result = $db->execute
      ('update table1 set value = 20');
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 3;
  is $result->table_name, undef;
  dies_ok { $result->all };
  dies_ok { $result->all_as_rows };
  dies_ok { $result->first };
  dies_ok { $result->first_as_row };
  my $invoked = 0;
  dies_ok { $result->each (sub { $invoked++ }) };
  dies_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 0;
} # _execute_update_return_multiple_rows_all

sub _execute_update_return_multiple_rows_error : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'testtable';
  my $db0 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db0->execute
      ('create table table1 (id int unsigned not null primary key,
                             value text)');
  $db0->execute
      ('insert into table1 (id, value)
        values (1253, NULL), (1424, "tex"), (10, "gseg aea")');

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  dies_ok { $db->execute ('update table1 set id = 20') };

  my $db2 = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0}});
  is $db2->execute
      ('select count(*) as count from table1 where id = 20')->first->{count},
      1; # Oh!
} # _execute_update_return_multiple_rows_error

sub _execute_delete_return_no_row_all : Test(10) {
  reset_db_set;
  my $dsn = test_dsn 'testtable';
  my $db0 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db0->execute
      ('create table table1 (id int unsigned not null primary key,
                             value text)');
  $db0->execute
      ('insert into table1 (id, value)
        values (1253, NULL), (1424, "tex"), (10, "gseg aea")');

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});

  my $result = $db->execute
      ('delete from table1 where value = "not found"');
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 0;
  is $result->table_name, undef;
  dies_ok { $result->all };
  dies_ok { $result->all_as_rows };
  dies_ok { $result->first };
  dies_ok { $result->first_as_row };
  my $invoked = 0;
  dies_ok { $result->each (sub { $invoked++ }) };
  dies_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 0;
} # _execute_delete_return_no_row_all

sub _execute_delete_return_a_row_all : Test(10) {
  reset_db_set;
  my $dsn = test_dsn 'testtable';
  my $db0 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db0->execute
      ('create table table1 (id int unsigned not null primary key,
                             value text)');
  $db0->execute
      ('insert into table1 (id, value)
        values (1253, NULL), (1424, "tex"), (10, "gseg aea")');

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});

  my $result = $db->execute
      ('delete from table1 where value = "tex"');
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 1;
  is $result->table_name, undef;
  dies_ok { $result->all };
  dies_ok { $result->all_as_rows };
  dies_ok { $result->first };
  dies_ok { $result->first_as_row };
  my $invoked = 0;
  dies_ok { $result->each (sub { $invoked++ }) };
  dies_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 0;
} # _execute_delete_return_a_row_all

sub _execute_delete_return_multiple_rows_all : Test(10) {
  reset_db_set;
  my $dsn = test_dsn 'testtable';
  my $db0 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db0->execute
      ('create table table1 (id int unsigned not null primary key,
                             value text)');
  $db0->execute
      ('insert into table1 (id, value)
        values (1253, NULL), (1424, "tex"), (10, "gseg aea")');

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});

  my $result = $db->execute ('delete from table1');
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 3;
  is $result->table_name, undef;
  dies_ok { $result->all };
  dies_ok { $result->all_as_rows };
  dies_ok { $result->first };
  dies_ok { $result->first_as_row };
  my $invoked = 0;
  dies_ok { $result->each (sub { $invoked++ }) };
  dies_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 0;
} # _execute_delete_return_multiple_rows_all

sub _execute_show_tables_return_multiple_rows_all : Test(10) {
  reset_db_set;
  my $dsn = test_dsn 'testtable';
  my $db0 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db0->execute
      ('create table table1 (id int unsigned not null primary key,
                             value text)');

  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 1}});

  my $result = $db->execute ('SHOW TABLES');
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 1;
  is $result->table_name, undef;
  eq_or_diff [values %{$result->all->to_a->[0]}], ['table1'];
  dies_ok { $result->all_as_rows };
  dies_ok { $result->first };
  dies_ok { $result->first_as_row };
  my $invoked = 0;
  dies_ok { $result->each (sub { $invoked++ }) };
  dies_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 0;
} # _execute_show_all_return_multiple_rows_all

__PACKAGE__->runtests;

1;

=head1 LICENSE

Copyright 2011 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
