package test::Dongry::Database::execute;
use strict;
use warnings;
no warnings 'utf8';
use Path::Class;
use lib file (__FILE__)->dir->parent->subdir ('lib')->stringify;
use Test::Dongry;
use base qw(Test::Class);
use Dongry::Database;
use Encode;

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

  dies_here_ok {
    $db->execute
        ('create table table1 (id int unsigned not null primary key)');
  };

  my $db2 = Dongry::Database->new
      (sources => {default => {dsn => $dsn}});
  dies_here_ok { $db2->execute ('select * from table1') };
} # _execute_create_table_no_master

sub _execute_create_table_not_writable : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'createtable';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 0}});

  dies_here_ok {
    $db->execute
        ('create table table1 (id int unsigned not null primary key)');
  };

  my $db2 = Dongry::Database->new
      (sources => {default => {dsn => $dsn}});
  dies_here_ok { $db2->execute ('select * from table1') };
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
  dies_here_ok {
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

  dies_here_ok {
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
  dies_here_ok { $result->each (sub { $invoked++ }) };
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 0;
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
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
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 0;
  lives_ok { $result->each (sub { $invoked++ }) };
  is $invoked, 0;
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
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
  dies_here_ok { $result->each (sub { $invoked++ }) };
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 0;
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
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
  dies_here_ok { $result->all_as_rows };
  my $all = $result->all;
  isa_list_n_ok $all, 0;
  my $invoked = 0;
  dies_here_ok { $result->each (sub { $invoked++ }) };
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 0;
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->all };
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
} # _execute_select_return_no_rows_all_as_rows

sub _execute_select_return_no_rows_first : Test(15) {
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
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
  my $invoked = 0;
  dies_here_ok { $result->each (sub { $invoked++ }) };
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 0;
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
  ok $result->is_success;
  ng $result->is_error;
  ng $result->error_text;
  ng $result->error_sql;
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
  dies_here_ok { $result->first_as_row };
  my $first = $result->first;
  is $first, undef;
  dies_here_ok { $result->first };
  my $invoked = 0;
  dies_here_ok { $result->each (sub { $invoked++ }) };
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 0;
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
} # _execute_select_return_no_rows_first_as_row

sub _execute_select_return_a_row_each : Test(16) {
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
  dies_here_ok { $result->each (sub { $invoked++ }) };
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 1;
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
  ok $result->is_success;
  ng $result->is_error;
  ng $result->error_text;
  ng $result->error_sql;
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
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 0;
  lives_ok { $result->each (sub { $invoked++ }) };
  is $invoked, 1;
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
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
  dies_here_ok { $result->each (sub { $invoked++ }) };
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 0;
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
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
  dies_here_ok { $result->all_as_rows };
  my $all = $result->all;
  isa_list_n_ok $all, 1;
  my $invoked = 0;
  dies_here_ok { $result->each (sub { $invoked++ }) };
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 0;
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->all };
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
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
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
  my $invoked = 0;
  dies_here_ok { $result->each (sub { $invoked++ }) };
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 0;
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
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
  dies_here_ok { $result->first_as_row };
  my $first = $result->first;
  eq_or_diff $first, {id => 1253, value => undef};
  dies_here_ok { $result->first };
  my $invoked = 0;
  dies_here_ok { $result->each (sub { $invoked++ }) };
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 0;
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
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
  dies_here_ok { $result->each (sub { $invoked++ }) };
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 3;
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
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
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 0;
  lives_ok { $result->each (sub { $invoked++ }) };
  is $invoked, 3;
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
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
  dies_here_ok { $result->each (sub { $invoked++ }) };
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 0;
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
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
  dies_here_ok { $result->all_as_rows };
  my $all = $result->all;
  isa_list_n_ok $all, 3;
  my $invoked = 0;
  dies_here_ok { $result->each (sub { $invoked++ }) };
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 0;
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->all };
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
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
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
  my $invoked = 0;
  dies_here_ok { $result->each (sub { $invoked++ }) };
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 0;
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
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
  dies_here_ok { $result->first_as_row };
  my $first = $result->first;
  eq_or_diff $first, {id => 10, value => 333};
  dies_here_ok { $result->first };
  my $invoked = 0;
  dies_here_ok { $result->each (sub { $invoked++ }) };
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 0;
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
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
  dies_here_ok { $result->each (sub { $invoked++ }) };
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 0;
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
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
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
  my $invoked = 0;
  dies_here_ok { $result->each (sub { $invoked++ }) };
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
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
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
  my $invoked = 0;
  dies_here_ok { $result->each (sub { $invoked++ }) };
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 0;
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
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
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
  my $invoked = 0;
  dies_here_ok { $result->each (sub { $invoked++ }) };
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
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
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
  my $invoked = 0;
  dies_here_ok { $result->each (sub { $invoked++ }) };
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
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
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
  my $invoked = 0;
  dies_here_ok { $result->each (sub { $invoked++ }) };
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
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
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
  my $invoked = 0;
  dies_here_ok { $result->each (sub { $invoked++ }) };
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
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
  dies_here_ok { $db->execute ('update table1 set id = 20') };

  my $db2 = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0}});
  like $db2->execute
      ('select count(*) as count from table1 where id = 20')->first->{count},
      qr{^(?:0|1)$};
     #0; # MySQL 5.5
     #1; # MySQL 5.0, 5.1
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
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
  my $invoked = 0;
  dies_here_ok { $result->each (sub { $invoked++ }) };
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
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
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
  my $invoked = 0;
  dies_here_ok { $result->each (sub { $invoked++ }) };
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
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
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
  my $invoked = 0;
  dies_here_ok { $result->each (sub { $invoked++ }) };
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
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
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
  my $invoked = 0;
  dies_here_ok { $result->each (sub { $invoked++ }) };
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 0;
} # _execute_show_all_return_multiple_rows_all

sub _execute_select_source : Test(4) {
  reset_db_set;
  my $dsn1 = test_dsn 'testdb1';
  my $dsn2 = test_dsn 'testdb2';

  my $db1 = Dongry::Database->new
      (sources => {master => {dsn => $dsn1, writable => 1}});
  $db1->execute ('create table foo (id int)');
  $db1->execute ('insert into foo (id) values (100)');
  my $db2 = Dongry::Database->new
      (sources => {master => {dsn => $dsn2, writable => 1}});
  $db2->execute ('create table foo (id int)');
  $db2->execute ('insert into foo (id) values (200)');

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn1, writable => 1},
                   default => {dsn => $dsn2, writable => 1},
                   heavy => {dsn => $dsn1}});
  is $db->execute ('select * from foo where id = 100')->row_count, 0;
  is $db->execute ('select * from foo where id = 100', [],
                   source_name => 'master')->row_count, 1;
  is $db->execute ('select * from foo where id = 100', [],
                   source_name => 'default')->row_count, 0;
  is $db->execute ('select * from foo where id = 100', [],
                   source_name => 'heavy')->row_count, 1;
} # _execute_select_source

sub _execute_insert_source : Test(3) {
  reset_db_set;
  my $dsn1 = test_dsn 'testdb1';
  my $dsn2 = test_dsn 'testdb2';
  my $dsn3 = test_dsn 'testdb3';

  my $db1 = Dongry::Database->new
      (sources => {master => {dsn => $dsn1, writable => 1}});
  $db1->execute ('create table foo (id int)');
  $db1->execute ('insert into foo (id) values (100)');
  my $db2 = Dongry::Database->new
      (sources => {master => {dsn => $dsn2, writable => 1}});
  $db2->execute ('create table foo (id int)');
  $db2->execute ('insert into foo (id) values (200)');
  my $db3 = Dongry::Database->new
      (sources => {master => {dsn => $dsn3, writable => 1}});
  $db3->execute ('create table foo (id int)');
  $db3->execute ('insert into foo (id) values (300)');

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn1, writable => 1},
                   default => {dsn => $dsn2, writable => 1},
                   heavy => {dsn => $dsn3, writable => 1}});
  $db->execute ('insert into foo (id) values (400)');
  
  $db1->disconnect;
  $db2->disconnect;
  $db3->disconnect;
  is $db1->execute ('select * from foo where id = 400', [],
                    source_name => 'master')->row_count, 1;
  is $db2->execute ('select * from foo where id = 400', [],
                    source_name => 'master')->row_count, 0;
  is $db3->execute ('select * from foo where id = 400', [],
                    source_name => 'master')->row_count, 0;
} # _execute_insert_source

sub _execute_insert_source_default : Test(3) {
  reset_db_set;
  my $dsn1 = test_dsn 'testdb1';
  my $dsn2 = test_dsn 'testdb2';
  my $dsn3 = test_dsn 'testdb3';

  my $db1 = Dongry::Database->new
      (sources => {master => {dsn => $dsn1, writable => 1}});
  $db1->execute ('create table foo (id int)');
  $db1->execute ('insert into foo (id) values (100)');
  my $db2 = Dongry::Database->new
      (sources => {master => {dsn => $dsn2, writable => 1}});
  $db2->execute ('create table foo (id int)');
  $db2->execute ('insert into foo (id) values (200)');
  my $db3 = Dongry::Database->new
      (sources => {master => {dsn => $dsn3, writable => 1}});
  $db3->execute ('create table foo (id int)');
  $db3->execute ('insert into foo (id) values (300)');

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn1, writable => 1},
                   default => {dsn => $dsn2, writable => 1},
                   heavy => {dsn => $dsn3, writable => 1}});
  $db->execute ('insert into foo (id) values (400)', [],
                source_name => 'default');
  
  $db1->disconnect;
  $db2->disconnect;
  $db3->disconnect;
  is $db1->execute ('select * from foo where id = 400', [],
                    source_name => 'master')->row_count, 0;
  is $db2->execute ('select * from foo where id = 400', [],
                    source_name => 'master')->row_count, 1;
  is $db3->execute ('select * from foo where id = 400', [],
                    source_name => 'master')->row_count, 0;
} # _execute_insert_source_default

sub _execute_insert_source_master : Test(3) {
  reset_db_set;
  my $dsn1 = test_dsn 'testdb1';
  my $dsn2 = test_dsn 'testdb2';
  my $dsn3 = test_dsn 'testdb3';

  my $db1 = Dongry::Database->new
      (sources => {master => {dsn => $dsn1, writable => 1}});
  $db1->execute ('create table foo (id int)');
  $db1->execute ('insert into foo (id) values (100)');
  my $db2 = Dongry::Database->new
      (sources => {master => {dsn => $dsn2, writable => 1}});
  $db2->execute ('create table foo (id int)');
  $db2->execute ('insert into foo (id) values (200)');
  my $db3 = Dongry::Database->new
      (sources => {master => {dsn => $dsn3, writable => 1}});
  $db3->execute ('create table foo (id int)');
  $db3->execute ('insert into foo (id) values (300)');

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn1, writable => 1},
                   default => {dsn => $dsn2, writable => 1},
                   heavy => {dsn => $dsn3, writable => 1}});
  $db->execute ('insert into foo (id) values (400)', [],
                source_name => 'master');
  
  $db1->disconnect;
  $db2->disconnect;
  $db3->disconnect;
  is $db1->execute ('select * from foo where id = 400', [],
                    source_name => 'master')->row_count, 1;
  is $db2->execute ('select * from foo where id = 400', [],
                    source_name => 'master')->row_count, 0;
  is $db3->execute ('select * from foo where id = 400', [],
                    source_name => 'master')->row_count, 0;
} # _execute_insert_source_master

sub _execute_insert_source_heavy : Test(3) {
  reset_db_set;
  my $dsn1 = test_dsn 'testdb1';
  my $dsn2 = test_dsn 'testdb2';
  my $dsn3 = test_dsn 'testdb3';

  my $db1 = Dongry::Database->new
      (sources => {master => {dsn => $dsn1, writable => 1}});
  $db1->execute ('create table foo (id int)');
  $db1->execute ('insert into foo (id) values (100)');
  my $db2 = Dongry::Database->new
      (sources => {master => {dsn => $dsn2, writable => 1}});
  $db2->execute ('create table foo (id int)');
  $db2->execute ('insert into foo (id) values (200)');
  my $db3 = Dongry::Database->new
      (sources => {master => {dsn => $dsn3, writable => 1}});
  $db3->execute ('create table foo (id int)');
  $db3->execute ('insert into foo (id) values (300)');

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn1, writable => 1},
                   default => {dsn => $dsn2, writable => 1},
                   heavy => {dsn => $dsn3, writable => 1}});
  $db->execute ('insert into foo (id) values (400)', [],
                source_name => 'heavy');
  
  $db1->disconnect;
  $db2->disconnect;
  $db3->disconnect;
  is $db1->execute ('select * from foo where id = 400', [],
                    source_name => 'master')->row_count, 0;
  is $db2->execute ('select * from foo where id = 400', [],
                    source_name => 'master')->row_count, 0;
  is $db3->execute ('select * from foo where id = 400', [],
                    source_name => 'master')->row_count, 1;
} # _execute_insert_source_heavy

sub _execute_update_source : Test(3) {
  reset_db_set;
  my $dsn1 = test_dsn 'testdb1';
  my $dsn2 = test_dsn 'testdb2';
  my $dsn3 = test_dsn 'testdb3';

  my $db1 = Dongry::Database->new
      (sources => {master => {dsn => $dsn1, writable => 1}});
  $db1->execute ('create table foo (id int)');
  $db1->execute ('insert into foo (id) values (100)');
  my $db2 = Dongry::Database->new
      (sources => {master => {dsn => $dsn2, writable => 1}});
  $db2->execute ('create table foo (id int)');
  $db2->execute ('insert into foo (id) values (200)');
  my $db3 = Dongry::Database->new
      (sources => {master => {dsn => $dsn3, writable => 1}});
  $db3->execute ('create table foo (id int)');
  $db3->execute ('insert into foo (id) values (300)');

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn1, writable => 1},
                   default => {dsn => $dsn2, writable => 1},
                   heavy => {dsn => $dsn1, writable => 1}});
  $db->execute ('UPDATE foo SET id = 400');
  
  $db1->disconnect;
  $db2->disconnect;
  $db3->disconnect;
  is $db1->execute ('select * from foo where id = 400', [],
                    source_name => 'master')->row_count, 1;
  is $db2->execute ('select * from foo where id = 400', [],
                    source_name => 'master')->row_count, 0;
  is $db3->execute ('select * from foo where id = 400', [],
                    source_name => 'master')->row_count, 0;
} # _execute_update_source

sub _execute_delete_source : Test(3) {
  reset_db_set;
  my $dsn1 = test_dsn 'testdb1';
  my $dsn2 = test_dsn 'testdb2';
  my $dsn3 = test_dsn 'testdb3';

  my $db1 = Dongry::Database->new
      (sources => {master => {dsn => $dsn1, writable => 1}});
  $db1->execute ('create table foo (id int)');
  $db1->execute ('insert into foo (id) values (100)');
  my $db2 = Dongry::Database->new
      (sources => {master => {dsn => $dsn2, writable => 1}});
  $db2->execute ('create table foo (id int)');
  $db2->execute ('insert into foo (id) values (200)');
  my $db3 = Dongry::Database->new
      (sources => {master => {dsn => $dsn3, writable => 1}});
  $db3->execute ('create table foo (id int)');
  $db3->execute ('insert into foo (id) values (300)');

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn1, writable => 1},
                   default => {dsn => $dsn2, writable => 1},
                   heavy => {dsn => $dsn1, writable => 1}});
  $db->execute ('DELETE from foo');
  
  $db1->disconnect;
  $db2->disconnect;
  $db3->disconnect;
  is $db1->execute ('select * from foo', [],
                    source_name => 'master')->row_count, 0;
  is $db2->execute ('select * from foo', [],
                    source_name => 'master')->row_count, 1;
  is $db3->execute ('select * from foo', [],
                    source_name => 'master')->row_count, 1;
} # _execute_delete_source

sub _execute_unknown_source_name : Test(1) {
  my $db = Dongry::Database->new;
  dies_here_ok { $db->execute ('select * from foo', [], source_name => 'foo') };
} # _execute_unknown_source_name

sub _execute_unknown_source_name_2 : Test(1) {
  my $db = Dongry::Database->new (sources => {master => {dsn => 'foo'}});
  dies_here_ok { $db->execute ('select * from foo', [], source_name => 'foo') };
} # _execute_unknown_source_name_2

sub _execute_bad_source_definition_1 : Test(1) {
  my $db = Dongry::Database->new
      (sources => {default => {dsn => 'bad'}});
  dies_here_ok { $db->execute ('select * from foo', []) };
} # _execute_bad_source_definition_1

sub _execute_bad_source_definition_2 : Test(1) {
  my $db = Dongry::Database->new
      (sources => {hoge => {dsn => 'bad'}});
  dies_here_ok { $db->execute ('select * from foo', [], source_name => 'hoge') };
} # _execute_bad_source_definition_2

sub _execute_bad_source_definition_3 : Test(1) {
  my $db = Dongry::Database->new
      (sources => {hoge => {dsn => 'dbi:mysql:bad'}});
  dies_here_ok { $db->execute ('select * from foo', [], source_name => 'hoge') };
} # _execute_bad_source_definition_3

sub _execute_must_be_writable_1 : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'hoge1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table hoge1 (id int)', [], source_name => 'master');
  $db->execute ('insert into hoge1 (id) values (1)');

  $db->disconnect;
  is $db->execute ('select * from hoge1', [],
                   must_be_writable => 1)->row_count, 1;
} # _execute_must_be_writable_1

sub _execute_must_be_writable_2 : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'hoge1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn},
                   writable => {dsn => $dsn, writable => 1}});
  $db->execute ('create table hoge1 (id int)', [], source_name => 'writable');
  $db->execute ('insert into hoge1 (id) values (1)', [],
                source_name => 'writable');

  $db->disconnect;
  dies_here_ok {
    $db->execute ('select * from hoge1', [], must_be_writable => 1);
  };
} # _execute_must_be_writable_2

sub _execute_must_be_writable_3 : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'hoge1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn},
                   default => {dsn => $dsn, writable => 1},
                   writable => {dsn => $dsn, writable => 1}});
  $db->execute ('create table hoge1 (id int)', [], source_name => 'writable');
  $db->execute ('insert into hoge1 (id) values (1)', [],
                source_name => 'writable');

  $db->disconnect;
  is $db->execute ('select * from hoge1', [],
                   must_be_writable => 1,
                   source_name => 'default')->row_count, 1;
} # _execute_must_be_writable_3

sub _execute_must_be_writable_4 : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'hoge1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn},
                   default => {dsn => $dsn, writable => 1},
                   writable => {dsn => $dsn, writable => 1}});
  $db->execute ('create table hoge1 (id int)', [], source_name => 'writable');

  $db->disconnect;
  is $db->execute ('insert into hoge1 (id) values (1)', [],
                   must_be_writable => 1,
                   source_name => 'default')->row_count, 1;
} # _execute_must_be_writable_4

sub _execute_must_be_writable_5 : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'hoge1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn},
                   writable => {dsn => $dsn, writable => 1}});
  $db->execute ('create table hoge1 (id int)', [], source_name => 'writable');

  $db->disconnect;
  dies_here_ok {
    $db->execute ('insert into hoge1 (id) values (1)', [],
                  must_be_writable => 1);
  };
} # _execute_must_be_writable_5

sub _execute_even_if_read_only_1 : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'hoge1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn},
                   writable => {dsn => $dsn, writable => 1}});
  $db->execute ('create table hoge1 (id int)', [], source_name => 'writable');

  $db->execute ('insert into hoge1 (id) values (1)', [],
                source_name => 'master',
                even_if_read_only => 1);

  $db->disconnect;
  is $db->execute ('select * from hoge1', [], source_name => 'writable')
      ->row_count, 1;
} # _execute_even_if_read_only_1

sub _execute_even_if_read_only_2 : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'hoge1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn},
                   default => {dsn => $dsn},
                   writable => {dsn => $dsn, writable => 1}});
  $db->execute ('create table hoge1 (id int)', [], source_name => 'writable');

  $db->execute ('insert into hoge1 (id) values (1)', [],
                even_if_read_only => 1);

  $db->disconnect;
  is $db->execute ('select * from hoge1', [], source_name => 'writable')
      ->row_count, 1;
} # _execute_even_if_read_only_2

sub _execute_even_if_read_only_3 : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'hoge1';
  my $db = Dongry::Database->new
      (sources => {fuga => {dsn => $dsn},
                   default => {dsn => $dsn},
                   writable => {dsn => $dsn, writable => 1}});
  $db->execute ('create table hoge1 (id int)', [], source_name => 'writable');

  $db->execute ('insert into hoge1 (id) values (1)', [],
                even_if_read_only => 1,
                source_name => 'fuga');

  $db->disconnect;
  is $db->execute ('select * from hoge1', [], source_name => 'writable')
      ->row_count, 1;
} # _execute_even_if_read_only_3

sub _execute_even_if_read_only_4 : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'hoge1';
  my $db = Dongry::Database->new
      (sources => {fuga => {dsn => $dsn},
                   default => {dsn => $dsn, writable => 1},
                   writable => {dsn => $dsn, writable => 1}});
  $db->execute ('create table hoge1 (id int)', [], source_name => 'writable');

  $db->execute ('insert into hoge1 (id) values (1)', [],
                even_if_read_only => 1,
                source_name => 'fuga');

  $db->disconnect;
  is $db->execute ('select * from hoge1', [], source_name => 'writable')
      ->row_count, 1;
} # _execute_even_if_read_only_4

sub _execute_even_if_read_only_5 : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'hoge1';
  my $db = Dongry::Database->new
      (sources => {fuga => {dsn => $dsn},
                   default => {dsn => $dsn},
                   writable => {dsn => $dsn, writable => 1}});
  $db->execute ('create table hoge1 (id int)', [], source_name => 'writable');

  $db->disconnect;
  is $db->execute ('select * from hoge1', [],
                   even_if_read_only => 1,
                   source_name => 'fuga')->row_count, 0;
} # _execute_even_if_read_only_5

sub _execute_even_if_read_only_default_source : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'hoge1';
  my $db = Dongry::Database->new
      (sources => {fuga => {dsn => $dsn},
                   master => {dsn => $dsn},
                   writable => {dsn => $dsn, writable => 1}});
  $db->execute ('create table hoge1 (id int)', [], source_name => 'writable');

  $db->disconnect;
  dies_here_ok {
    $db->execute ('select * from hoge1', [],
                  even_if_read_only => 1);
  };
} # _execute_even_if_read_only_default_source

sub _execute_even_if_read_only_default_source_2 : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'hoge1';
  my $db = Dongry::Database->new
      (sources => {fuga => {dsn => $dsn},
                   default => {dsn => $dsn},
                   writable => {dsn => $dsn, writable => 1}});
  $db->execute ('create table hoge1 (id int)', [], source_name => 'writable');

  dies_here_ok {
    $db->execute ('insert into hoge1 (id) values (0)', [],
                  even_if_read_only => 1);
  };
} # _execute_even_if_read_only_default_source_2

sub _execute_placeholder_zero : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'hoge1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn},
                   writable => {dsn => $dsn, writable => 1}});
  $db->execute ('create table hoge1 (id int)', undef,
                source_name => 'writable');
  $db->execute ('insert into hoge1 (id) values (12)', undef,
                source_name => 'writable');

  is $db->execute ('select * from hoge1', [])->row_count, 1;
} # _execute_placeholder_zero

sub _execute_placeholder_one : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'hoge1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn},
                   writable => {dsn => $dsn, writable => 1}});
  $db->execute ('create table hoge1 (id int)', undef,
                source_name => 'writable');
  $db->execute ('insert into hoge1 (id) values (12)', undef,
                source_name => 'writable');

  is $db->execute ('select * from hoge1 where id = ?', [12])->row_count, 1;
} # _execute_placeholder_one

sub _execute_placeholder_one_2 : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'hoge1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn},
                   writable => {dsn => $dsn, writable => 1}});
  $db->execute ('create table hoge1 (id int)', undef,
                source_name => 'writable');
  $db->execute ('insert into hoge1 (id) values (12)', undef,
                source_name => 'writable');

  is $db->execute ('select * from hoge1 where id = ?', [123])->row_count, 0;
} # _execute_placeholder_one_2

sub _execute_placeholder_multiple : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'hoge1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn},
                   writable => {dsn => $dsn, writable => 1}});
  $db->execute ('create table hoge1 (id int, value text)', undef,
                source_name => 'writable');
  $db->execute ('insert into hoge1 (id,value) values (12,"ab")', undef,
                source_name => 'writable');
  $db->execute ('insert into hoge1 (id,value) values (13,null)', undef,
                source_name => 'writable');

  is $db->execute ('select * from hoge1 where id > ? and value = ?',
                   [10, "ab"])->row_count, 1;
} # _execute_placeholder_multiple

sub _execute_placeholder_multiple_2 : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'hoge1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn},
                   writable => {dsn => $dsn, writable => 1}});
  $db->execute ('create table hoge1 (id int, value text)', undef,
                source_name => 'writable');
  $db->execute ('insert into hoge1 (id,value) values (12,"ab")', undef,
                source_name => 'writable');
  $db->execute ('insert into hoge1 (id,value) values (13,null)', undef,
                source_name => 'writable');

  is $db->execute ('select * from hoge1 where id > ? and value = ?',
                   [10, undef])->row_count, 0;
} # _execute_placeholder_multiple_2

sub _execute_placeholder_multiple_3 : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'hoge1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn},
                   writable => {dsn => $dsn, writable => 1}});
  $db->execute ('create table hoge1 (id int, value text)', undef,
                source_name => 'writable');
  $db->execute ('insert into hoge1 (id,value) values (12,"ab")', undef,
                source_name => 'writable');
  $db->execute ('insert into hoge1 (id,value) values (13,null)', undef,
                source_name => 'writable');
  $db->execute ('insert into hoge1 (id,value) values (13,"")', undef,
                source_name => 'writable');

  is $db->execute ('select * from hoge1 where id > ? and value = ?',
                   [10, undef])->row_count, 0;
} # _execute_placeholder_multiple_3

sub _execute_placeholder_multiple_4 : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'hoge1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn},
                   writable => {dsn => $dsn, writable => 1}});
  $db->execute ('create table hoge1 (id int, value text)', undef,
                source_name => 'writable');

  $db->execute ('insert into hoge1 (id,value) values (?, ?)',
                [10, undef],
                source_name => 'writable');

  is $db->execute ('select * from hoge1 where id = ? and value is null',
                   [10])->row_count, 1;
} # _execute_placeholder_multiple_4

sub _execute_placeholder_too_many : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'hoge1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn},
                   writable => {dsn => $dsn, writable => 1}});
  $db->execute ('create table hoge1 (id int, value text)', undef,
                source_name => 'writable');

  dies_here_ok { 
    $db->execute ('insert into hoge1 (id,value) values (?, ?)',
                  [10, undef, 4],
                  source_name => 'writable');
  };

  is $db->execute ('select * from hoge1 where id = ? and value is null',
                   [10])->row_count, 0;
} # _execute_placeholder_too_many

sub _execute_placeholder_too_few : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'hoge1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn},
                   writable => {dsn => $dsn, writable => 1}});
  $db->execute ('create table hoge1 (id int, value text)', undef,
                source_name => 'writable');

  dies_here_ok { 
    $db->execute ('insert into hoge1 (id,value) values (?, ?)',
                  [10],
                  source_name => 'writable');
  };

  is $db->execute ('select * from hoge1 where id = ? and value is null',
                   [10])->row_count, 0;
} # _execute_placeholder_too_few

sub _execute_placeholder_bytes : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'hoge1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn},
                   writable => {dsn => $dsn, writable => 1}});
  $db->execute ('create table hoge1 (id int, value text)', undef,
                source_name => 'writable');
  
  my $text = encode 'utf-8', "\x{1000}";
  $db->execute ('insert into hoge1 (id,value) values (?, ?)',
                [10, $text],
                source_name => 'writable');

  is $db->execute ('select * from hoge1 where id = 10')->first->{value}, $text;
} # _execute_placeholder_bytes

sub _execute_bytes : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'hoge1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn},
                   writable => {dsn => $dsn, writable => 1}});
  $db->execute ('create table hoge1 (id int, value text)', undef,
                source_name => 'writable');
  my $text = encode 'utf-8', "\x{1000}";
  $db->execute ('insert into hoge1 (id,value) values (?, ?)',
                [10, $text],
                source_name => 'writable');

  is $db->execute ('select * from hoge1 where value = "' . $text . '"')
      ->first->{id}, 10;
} # _execute_bytes

sub _execute_named_no_args : Test(2) {
  my $db = new_db;
  my $result = $db->execute ('create table foo (id int)', {});
  is $db->{last_sql}, 'create table foo (id int)';
  isa_ok $result, 'Dongry::Database::Executed';
} # _execute_named_no_args

sub _execute_named_with_args : Test(2) {
  my $db = new_db;
  my $result = $db->execute ('create table foo (:col:id int)', {col => 'id'});
  is $db->{last_sql}, 'create table foo (`id` int)';
  isa_ok $result, 'Dongry::Database::Executed';
} # _execute_named_with_args

sub _execute_named_with_args_missing : Test(2) {
  my $db = new_db;
  local $DBIx::ShowSQL::COUNT = 1;
  local $DBIx::ShowSQL::SQLCount = 0;
  dies_here_ok {
    my $result = $db->execute ('create table foo (:col:id int)', {});
  };
  is $DBIx::ShowSQL::SQLCount, 0;
} # _execute_named_with_args_missing

sub _execute_named_with_args_too_many : Test(2) {
  my $db = new_db;
  local $DBIx::ShowSQL::COUNT = 1;
  local $DBIx::ShowSQL::SQLCount = 0;
  dies_here_ok {
    my $result = $db->execute ('create table foo (id int)', {hoge => 1});
  };
  is $DBIx::ShowSQL::SQLCount, 0;
} # _execute_named_with_args_too_many

sub _execute_named_sub : Test(2) {
  my $db = new_db;
  $db->execute ('create table foo (id int)', {});
  $db->insert ('foo', [{id => 123}, {id => 123}]);

  my $result = $db->execute
      ('select * from foo where :sub:sub', {sub => {id => 123}});
  is $db->{last_sql}, 'select * from foo where (`id` = ?)';
  is $result->row_count, 2;
} # _execute_named_sub

sub _execute_utf8_flagged : Test(1) {
  my $db = new_db;
  $db->execute ('create table foo (value BLOB)');

  $db->execute (qq{insert into foo (value) VALUE ("\x{4500}")});
  is $db->execute ('select * from foo')
      ->first->{value}, encode 'utf-8', "\x{4500}";
} # _execute_utf8_flagged

sub _execute_utf8_unflagged : Test(1) {
  my $db = new_db;
  $db->execute ('create table foo (value BLOB)');

  $db->execute (encode 'utf-8', qq{insert into foo (value) VALUE ("\x{450}")});
  is $db->execute ('select * from foo')
      ->first->{value}, encode 'utf-8', "\x{450}";
} # _execute_utf8_unflagged

sub _execute_non_utf8_unflagged : Test(1) {
  my $db = new_db;
  $db->execute ('create table foo (value BLOB)');

  $db->execute (qq{insert into foo (value) VALUE ("\x81\xD2\x94a")});
  is $db->execute ('select * from foo')
      ->first->{value}, "\x81\xD2\x94a";
} # _execute_non_utf8_unflagged

sub _execute_utf8_flagged_value : Test(1) {
  my $db = new_db;
  $db->execute ('create table foo (value BLOB)');

  $db->execute (qq{insert into foo (value) VALUE (?)}, ["\x{4500}"]);
  is $db->execute ('select * from foo')
      ->first->{value}, encode 'utf-8', "\x{4500}";
} # _execute_utf8_flagged_value

sub _execute_utf8_flagged_value_2 : Test(1) {
  my $db = new_db;
  $db->execute ('create table foo (value BLOB)');

  $db->execute (qq{insert into foo (value) VALUE (?)}, ["\x{D800}"]);
  is $db->execute ('select * from foo')
      ->first->{value}, encode 'utf8', "\x{D800}";
} # _execute_utf8_flagged_value_2

sub _execute_utf8_unflagged_value : Test(1) {
  my $db = new_db;
  $db->execute ('create table foo (value BLOB)');

  $db->execute (qq{insert into foo (value) VALUE (?)},
                [encode 'utf-8', "\x{4500}"]);
  is $db->execute ('select * from foo')
      ->first->{value}, encode 'utf-8', "\x{4500}";
} # _execute_utf8_unflagged_value

sub _execute_non_utf8_value : Test(1) {
  my $db = new_db;
  $db->execute ('create table foo (value BLOB)');

  $db->execute (qq{insert into foo (value) VALUE (?)}, ["\x98\xDCz"]);
  is $db->execute ('select * from foo')
      ->first->{value}, "\x98\xDCz";
} # _execute_non_utf8_value

sub _execute_callback : Test(6) {
  my $db = new_db;
  $db->execute ('create table foo (id int)');

  my $result;
  $db->execute ('insert into foo (id) values (43), (31)', undef, cb => sub {
    is $_[0], $db;
    $result = $_[1];
  });

  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  is $result->row_count, 2;
  dies_ok { $result->all };
} # _execute_callback

sub _execute_callback_fetch : Test(6) {
  my $db = new_db;
  $db->execute ('create table foo (id int)');
  $db->execute ('insert into foo (id) values (5), (543)');

  my $result;
  $db->execute ('select * from foo order by id asc', undef, cb => sub {
    is $_[0], $db;
    $result = $_[1];
  });

  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  is $result->row_count, 2;
  eq_or_diff $result->all->to_a, [{id => 5}, {id => 543}];
} # _execute_callback_fetch

sub _execute_callback_error : Test(2) {
  my $db = new_db;
  $db->execute ('create table foo (id int)');

  my $result;
  dies_here_ok {
    $db->execute ('insert into syntax error', undef, cb => sub {
      $result = $_[1];
    });
  };

  is $result, undef;
} # _execute_callback_error

sub _execute_callback_and_return : Test(8) {
  my $db = new_db;
  $db->execute ('create table foo (id int)');

  my $result;
  my $result2 = $db->execute
      ('insert into foo (id) values (43), (31)', undef, cb => sub {
    is $_[0], $db;
    $result = $_[1];
  });

  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  is $result->row_count, 2;
  dies_ok { $result->all };

  is $result2, $result;
  is $result2->row_count, 2;
} # _execute_callback_and_return

sub _execute_callback_croak : Test(2) {
  my $db = new_db;
  $db->execute ('create table foo (id int)');

  eval {
    $db->execute ('insert into foo (id) values (43), (31)', undef, cb => sub {
      Carp::croak "hoge";
    });
  };
  is $@, 'hoge at ' . __FILE__ . ' line ' . (__LINE__ - 2) . "\n";

  eq_or_diff $db->execute ('select * from foo order by id asc')->all->to_a,
      [{id => 31}, {id => 43}];
} # _execute_callback_croak

sub _execute_callback_die : Test(2) {
  my $db = new_db;
  $db->execute ('create table foo (id int)');

  eval {
    $db->execute ('insert into foo (id) values (43), (31)', undef, cb => sub {
      die "hoge";
    });
  };
  is $@, 'hoge at ' . __FILE__ . ' line ' . (__LINE__ - 3) . ".\n";

  eq_or_diff $db->execute ('select * from foo order by id asc')->all->to_a,
      [{id => 31}, {id => 43}];
} # _execute_callback_die

{
  package MyListClass;
  $INC{'MyListClass.pm'} = 1;
  sub new {
    return bless $_[1] || [], $_[0];
  } # new
  sub map {
    return bless [CORE::map { $_[1]->() } @{$_[0]}], ref $_[0];
  } # map
}

sub _result_all_custom_list : Test(2) {
  local $Dongry::Database::ListClass = 'MyListClass';
  my $db = new_db;
  $db->execute ('create table foo (id int)');
  $db->execute ('insert into foo (id) values (1), (2)');
  my $list = $db->execute ('select * from foo order by id asc')->all;
  isa_ok $list, 'MyListClass';
  eq_or_diff [@$list], [{id => 1}, {id => 2}];
} # _result_all_custom_list

sub _result_all_as_rows_custom_list : Test(2) {
  local $Dongry::Database::ListClass = 'MyListClass';
  my $db = new_db;
  $db->execute ('create table foo (id int)');
  $db->execute ('insert into foo (id) values (1), (2)');
  my $list = $db->select ('foo', ['1 = 1'], order => [id => 1])->all_as_rows;
  isa_ok $list, 'MyListClass';
  eq_or_diff [map { $_->{data} } @$list], [{id => 1}, {id => 2}];
} # _result_all_as_rows_custom_list

__PACKAGE__->runtests;

1;

=head1 LICENSE

Copyright 2011-2012 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
