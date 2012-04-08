package test::Dongry::Database::insert;
use strict;
use warnings;
use Path::Class;
use lib file (__FILE__)->dir->parent->subdir ('lib')->stringify;
use Test::Dongry;
use base qw(Test::Class);
use Dongry::Database;
use Encode;
use List::Rubyish;

sub _insert_a_row : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');
  
  $db->insert ('foo', [{id => 1243, v1 => "hoge", v2 => undef}]);

  my $result = $db->execute ('select * from foo');
  is $result->row_count, 1;
  eq_or_diff $result->first, {id => 1243, v1 => "hoge", v2 => undef};
} # _insert_a_row

sub _insert_two_rows : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');
  
  $db->insert ('foo', [{id => 1243, v1 => "hoge"},
                       {id => 2511, v2 => 'fuga'}]);

  my $result = $db->execute ('select * from foo order by id asc');
  is $result->row_count, 2;
  eq_or_diff $result->all->to_a,
      [{id => 1243, v1 => "hoge", v2 => undef},
       {id => 2511, v1 => undef, v2 => 'fuga'}];
} # _insert_two_rows

sub _insert_zero_rows : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');
  
  dies_here_ok { $db->insert ('foo', []) };

  my $result = $db->execute ('select * from foo order by id asc');
  is $result->row_count, 0;
} # _insert_zero_rows

sub _insert_has_no_cols : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');
  
  $db->insert ('foo', [{}]);

  my $result = $db->execute ('select * from foo order by id asc');
  is $result->row_count, 1;
  eq_or_diff $result->first, {id => undef, v1 => undef, v2 => undef};
} # _insert_has_no_cols

sub _insert_not_writable : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 0},
                   default => {dsn => $dsn},
                   writable => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 text, v2 text)', [],
                source_name => 'writable');
  
  dies_here_ok { $db->insert ('foo', [{id => 31}]) };

  my $result = $db->execute ('select * from foo order by id asc');
  is $result->row_count, 0;
} # _insert_not_writable

sub _insert_source_not_specified : Test(3) {
  reset_db_set;

  my $dsn1 = test_dsn 'test1';
  my $dsn2 = test_dsn 'test2';
  my $dsn3 = test_dsn 'test3';

  my $db1 = Dongry::Database->new
      (sources => {master => {dsn => $dsn1, writable => 1}});
  my $db2 = Dongry::Database->new
      (sources => {master => {dsn => $dsn2, writable => 1}});
  my $db3 = Dongry::Database->new
      (sources => {master => {dsn => $dsn3, writable => 1}});

  $db1->execute ('create table foo (id int, v1 text)');
  $db2->execute ('create table foo (id int, v1 text)');
  $db3->execute ('create table foo (id int, v1 text)');

  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn1, writable => 1},
                   master => {dsn => $dsn2, writable => 1},
                   heavy => {dsn => $dsn3, writable => 1}});

  $db->insert ('foo', [{id => 3111}]);
  
  $db1->disconnect;
  $db2->disconnect;
  $db3->disconnect;
  is $db1->execute ('select * from foo', [],
                    source_name => 'master')->row_count, 0;
  is $db2->execute ('select * from foo', [],
                    source_name => 'master')->row_count, 1;
  is $db3->execute ('select * from foo', [],
                    source_name => 'master')->row_count, 0;
} # _insert_source_not_specified

sub _insert_source_master : Test(3) {
  reset_db_set;

  my $dsn1 = test_dsn 'test1';
  my $dsn2 = test_dsn 'test2';
  my $dsn3 = test_dsn 'test3';

  my $db1 = Dongry::Database->new
      (sources => {master => {dsn => $dsn1, writable => 1}});
  my $db2 = Dongry::Database->new
      (sources => {master => {dsn => $dsn2, writable => 1}});
  my $db3 = Dongry::Database->new
      (sources => {master => {dsn => $dsn3, writable => 1}});

  $db1->execute ('create table foo (id int, v1 text)');
  $db2->execute ('create table foo (id int, v1 text)');
  $db3->execute ('create table foo (id int, v1 text)');

  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn1, writable => 1},
                   master => {dsn => $dsn2, writable => 1},
                   heavy => {dsn => $dsn3, writable => 1}});

  $db->insert ('foo', [{id => 3111}], source_name => 'master');
  
  $db1->disconnect;
  $db2->disconnect;
  $db3->disconnect;
  is $db1->execute ('select * from foo', [],
                    source_name => 'master')->row_count, 0;
  is $db2->execute ('select * from foo', [],
                    source_name => 'master')->row_count, 1;
  is $db3->execute ('select * from foo', [],
                    source_name => 'master')->row_count, 0;
} # _insert_source_master

sub _insert_source_default : Test(3) {
  reset_db_set;

  my $dsn1 = test_dsn 'test1';
  my $dsn2 = test_dsn 'test2';
  my $dsn3 = test_dsn 'test3';

  my $db1 = Dongry::Database->new
      (sources => {master => {dsn => $dsn1, writable => 1}});
  my $db2 = Dongry::Database->new
      (sources => {master => {dsn => $dsn2, writable => 1}});
  my $db3 = Dongry::Database->new
      (sources => {master => {dsn => $dsn3, writable => 1}});

  $db1->execute ('create table foo (id int, v1 text)');
  $db2->execute ('create table foo (id int, v1 text)');
  $db3->execute ('create table foo (id int, v1 text)');

  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn1, writable => 1},
                   master => {dsn => $dsn2, writable => 1},
                   heavy => {dsn => $dsn3, writable => 1}});

  $db->insert ('foo', [{id => 3111}], source_name => 'default');
  
  $db1->disconnect;
  $db2->disconnect;
  $db3->disconnect;
  is $db1->execute ('select * from foo', [],
                    source_name => 'master')->row_count, 1;
  is $db2->execute ('select * from foo', [],
                    source_name => 'master')->row_count, 0;
  is $db3->execute ('select * from foo', [],
                    source_name => 'master')->row_count, 0;
} # _insert_source_default

sub _insert_source_heavy : Test(3) {
  reset_db_set;

  my $dsn1 = test_dsn 'test1';
  my $dsn2 = test_dsn 'test2';
  my $dsn3 = test_dsn 'test3';

  my $db1 = Dongry::Database->new
      (sources => {master => {dsn => $dsn1, writable => 1}});
  my $db2 = Dongry::Database->new
      (sources => {master => {dsn => $dsn2, writable => 1}});
  my $db3 = Dongry::Database->new
      (sources => {master => {dsn => $dsn3, writable => 1}});

  $db1->execute ('create table foo (id int, v1 text)');
  $db2->execute ('create table foo (id int, v1 text)');
  $db3->execute ('create table foo (id int, v1 text)');

  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn1, writable => 1},
                   master => {dsn => $dsn2, writable => 1},
                   heavy => {dsn => $dsn3, writable => 1}});

  $db->insert ('foo', [{id => 3111}], source_name => 'heavy');
  
  $db1->disconnect;
  $db2->disconnect;
  $db3->disconnect;
  is $db1->execute ('select * from foo', [],
                    source_name => 'master')->row_count, 0;
  is $db2->execute ('select * from foo', [],
                    source_name => 'master')->row_count, 0;
  is $db3->execute ('select * from foo', [],
                    source_name => 'master')->row_count, 1;
} # _insert_source_heavy

sub _insert_source_not_defined : Test(1) {
  my $db = Dongry::Database->new;
  dies_here_ok { $db->insert ('foo', [{id => 444}]) };
} # _insert_source_not_defined

sub _insert_source_not_found : Test(1) {
  my $db = Dongry::Database->new
      (sources => {hoge => {dsn => 'foo'}});
  dies_here_ok { $db->insert ('foo', [{id => 444}], source_name => 'fuga') };
} # _insert_source_not_found

sub _insert_bad_value_arg : Test(3) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');
  
  dies_ok { $db->insert ('foo', {id => 1243, v1 => "hoge", v2 => undef}) };
  like $db->{last_sql}, qr[^create];

  my $result = $db->execute ('select * from foo');
  is $result->row_count, 0;
} # _insert_bad_value_arg

sub _insert_bad_value_arg_2 : Test(3) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');
  
  dies_ok { $db->insert ('foo') };
  like $db->{last_sql}, qr[^create];

  my $result = $db->execute ('select * from foo');
  is $result->row_count, 0;
} # _insert_bad_value_arg_2

sub _insert_bad_value_arg_3 : Test(3) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');
  
  dies_ok { $db->insert ({id => 31}) };
  like $db->{last_sql}, qr[^create];

  my $result = $db->execute ('select * from foo');
  is $result->row_count, 0;
} # _insert_bad_value_arg_3

sub _insert_bad_value_arg_4 : Test(3) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');

  dies_ok { $db->insert ({id => 31}, source_name => 'master', 'dummy') };
  like $db->{last_sql}, qr[^create];

  my $result = $db->execute ('select * from foo');
  is $result->row_count, 0;
} # _insert_bad_value_arg_4

sub _insert_a_row_result_each : Test(12) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');
  
  my $result = $db->insert ('foo', [{id => 32}]);
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 1;
  is $result->table_name, 'foo';
  my @value;
  $result->each (sub { push @value, $_ });
  eq_or_diff \@value, [{id => 32}];
  @value = ();
  dies_here_ok { $result->each (sub { push @value, $_ }) };
  dies_here_ok { $result->each_as_row (sub { push @value, $_ }) };
  eq_or_diff \@value, [];
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
} # _insert_a_row_result_each

sub _insert_a_row_result_each_as_row : Test(16) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');
  
  my $result = $db->insert ('foo', [{id => 32}]);
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 1;
  is $result->table_name, 'foo';
  my @value;
  $result->each_as_row (sub { push @value, $_ });
  is 0+@value, 1;
  isa_ok $value[0], 'Dongry::Table::Row';
  is $value[0]->{db}, $db;
  is $value[0]->{table_name}, 'foo';
  eq_or_diff $value[0]->{data}, {id => 32};
  is $value[0]->{parsed_data}, undef;
  @value = ();
  dies_here_ok { $result->each_as_row (sub { push @value, $_ }) };
  dies_here_ok { $result->each (sub { push @value, $_ }) };
  eq_or_diff \@value, [];
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
} # _insert_a_row_result_each_as_row

sub _insert_a_row_result_all : Test(12) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');
  
  my $result = $db->insert ('foo', [{id => 32}]);
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 1;
  is $result->table_name, 'foo';
  my $values = $result->all;
  isa_list_n_ok $values, 1;
  eq_or_diff $values->to_a, [{id => 32}];
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
  my @value;
  dies_here_ok { $result->each (sub { push @value, $_ }) };
  dies_here_ok { $result->each_as_row (sub { push @value, $_ }) };
  eq_or_diff \@value, [];
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
} # _insert_a_row_result_all

sub _insert_a_row_result_all_as_rows : Test(16) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');
  
  my $result = $db->insert ('foo', [{id => 32}]);
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 1;
  is $result->table_name, 'foo';
  my $values = $result->all_as_rows;
  isa_list_n_ok $values, 1;
  isa_ok $values->[0], 'Dongry::Table::Row';
  is $values->[0]->{db}, $db;
  is $values->[0]->{table_name}, 'foo';
  eq_or_diff $values->[0]->{data}, {id => 32};
  is $values->[0]->{parsed_data}, undef;
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->all };
  my @value;
  dies_here_ok { $result->each_as_row (sub { push @value, $_ }) };
  dies_here_ok { $result->each (sub { push @value, $_ }) };
  eq_or_diff \@value, [];
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
} # _insert_a_row_result_all_as_rows

sub _insert_a_row_result_first : Test(12) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');
  
  my $result = $db->insert ('foo', [{id => 32}]);
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 1;
  is $result->table_name, 'foo';
  my $value = $result->first;
  eq_or_diff $value, {id => 32};
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
  my @value;
  dies_here_ok { $result->each (sub { push @value, $_ }) };
  dies_here_ok { $result->each_as_row (sub { push @value, $_ }) };
  eq_or_diff \@value, [];
} # _insert_a_row_result_first

sub _insert_a_row_result_first_as_row : Test(16) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');
  
  my $result = $db->insert ('foo', [{id => 32}]);
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 1;
  is $result->table_name, 'foo';
  my $value = $result->first_as_row;
  isa_ok $value, 'Dongry::Table::Row';
  is $value->{db}, $db;
  is $value->{table_name}, 'foo';
  eq_or_diff $value->{data}, {id => 32};
  is $value->{parsed_data}, undef;
  dies_here_ok { $result->first_as_row };
  dies_here_ok { $result->first };
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->all };
  my @value;
  dies_here_ok { $result->each_as_row (sub { push @value, $_ }) };
  dies_here_ok { $result->each (sub { push @value, $_ }) };
  eq_or_diff \@value, [];
} # _insert_a_row_result_first_as_row

sub _insert_a_row_result_each_zero : Test(12) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table `0` (id int, v1 text, v2 text)');
  
  my $result = $db->insert ('0', [{id => 32}]);
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 1;
  is $result->table_name, '0';
  my @value;
  $result->each (sub { push @value, $_ });
  eq_or_diff \@value, [{id => 32}];
  @value = ();
  dies_here_ok { $result->each (sub { push @value, $_ }) };
  dies_here_ok { $result->each_as_row (sub { push @value, $_ }) };
  eq_or_diff \@value, [];
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
} # _insert_a_row_result_each_zero

sub _insert_a_row_result_each_as_row_zero : Test(16) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table `0` (id int, v1 text, v2 text)');
  
  my $result = $db->insert ('0', [{id => 32}]);
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 1;
  is $result->table_name, '0';
  my @value;
  $result->each_as_row (sub { push @value, $_ });
  is 0+@value, 1;
  isa_ok $value[0], 'Dongry::Table::Row';
  is $value[0]->{db}, $db;
  is $value[0]->{table_name}, '0';
  eq_or_diff $value[0]->{data}, {id => 32};
  is $value[0]->{parsed_data}, undef;
  @value = ();
  dies_here_ok { $result->each_as_row (sub { push @value, $_ }) };
  dies_here_ok { $result->each (sub { push @value, $_ }) };
  eq_or_diff \@value, [];
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
} # _insert_a_row_result_each_as_row_zero

sub _insert_a_row_result_all_zero : Test(12) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table `0` (id int, v1 text, v2 text)');
  
  my $result = $db->insert ('0', [{id => 32}]);
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 1;
  is $result->table_name, '0';
  my $values = $result->all;
  isa_list_n_ok $values, 1;
  eq_or_diff $values->to_a, [{id => 32}];
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
  my @value;
  dies_here_ok { $result->each (sub { push @value, $_ }) };
  dies_here_ok { $result->each_as_row (sub { push @value, $_ }) };
  eq_or_diff \@value, [];
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
} # _insert_a_row_result_all_zero

sub _insert_a_row_result_all_as_rows_zero : Test(16) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table `0` (id int, v1 text, v2 text)');
  
  my $result = $db->insert ('0', [{id => 32}]);
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 1;
  is $result->table_name, '0';
  my $values = $result->all_as_rows;
  isa_list_n_ok $values, 1;
  isa_ok $values->[0], 'Dongry::Table::Row';
  is $values->[0]->{db}, $db;
  is $values->[0]->{table_name}, '0';
  eq_or_diff $values->[0]->{data}, {id => 32};
  is $values->[0]->{parsed_data}, undef;
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->all };
  my @value;
  dies_here_ok { $result->each_as_row (sub { push @value, $_ }) };
  dies_here_ok { $result->each (sub { push @value, $_ }) };
  eq_or_diff \@value, [];
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
} # _insert_a_row_result_all_as_rows_zero

sub _insert_a_row_result_first_zero : Test(12) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table `0` (id int, v1 text, v2 text)');
  
  my $result = $db->insert ('0', [{id => 32}]);
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 1;
  is $result->table_name, '0';
  my $value = $result->first;
  eq_or_diff $value, {id => 32};
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
  my @value;
  dies_here_ok { $result->each (sub { push @value, $_ }) };
  dies_here_ok { $result->each_as_row (sub { push @value, $_ }) };
  eq_or_diff \@value, [];
} # _insert_a_row_result_first_zero

sub _insert_a_row_result_first_as_row_zero : Test(16) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table `0` (id int, v1 text, v2 text)');
  
  my $result = $db->insert ('0', [{id => 32}]);
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 1;
  is $result->table_name, '0';
  my $value = $result->first_as_row;
  isa_ok $value, 'Dongry::Table::Row';
  is $value->{db}, $db;
  is $value->{table_name}, '0';
  eq_or_diff $value->{data}, {id => 32};
  is $value->{parsed_data}, undef;
  dies_here_ok { $result->first_as_row };
  dies_here_ok { $result->first };
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->all };
  my @value;
  dies_here_ok { $result->each_as_row (sub { push @value, $_ }) };
  dies_here_ok { $result->each (sub { push @value, $_ }) };
  eq_or_diff \@value, [];
} # _insert_a_row_result_first_as_row_zero

sub _insert_two_rows_result_each : Test(12) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');
  
  my $result = $db->insert ('foo', [{id => 32}, {id => 53}]);
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 2;
  is $result->table_name, 'foo';
  my @value;
  $result->each (sub { push @value, $_ });
  eq_or_diff \@value, [{id => 32}, {id => 53}];
  @value = ();
  dies_here_ok { $result->each (sub { push @value, $_ }) };
  dies_here_ok { $result->each_as_row (sub { push @value, $_ }) };
  eq_or_diff \@value, [];
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
} # _insert_two_rows_result_each

sub _insert_two_rows_result_each_as_row : Test(21) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');
  
  my $result = $db->insert ('foo', [{id => 32}, {id => 53}]);
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 2;
  is $result->table_name, 'foo';
  my @value;
  $result->each_as_row (sub { push @value, $_ });
  is 0+@value, 2;
  isa_ok $value[0], 'Dongry::Table::Row';
  is $value[0]->{db}, $db;
  is $value[0]->{table_name}, 'foo';
  eq_or_diff $value[0]->{data}, {id => 32};
  is $value[0]->{parsed_data}, undef;
  isa_ok $value[1], 'Dongry::Table::Row';
  is $value[1]->{db}, $db;
  is $value[1]->{table_name}, 'foo';
  eq_or_diff $value[1]->{data}, {id => 53};
  is $value[1]->{parsed_data}, undef;
  @value = ();
  dies_here_ok { $result->each_as_row (sub { push @value, $_ }) };
  dies_here_ok { $result->each (sub { push @value, $_ }) };
  eq_or_diff \@value, [];
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
} # _insert_two_rows_result_each_as_row

sub _insert_two_rows_result_all : Test(12) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');
  
  my $result = $db->insert ('foo', [{id => 32}, {id => 52}]);
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 2;
  is $result->table_name, 'foo';
  my $values = $result->all;
  isa_list_n_ok $values, 2;
  eq_or_diff $values->to_a, [{id => 32}, {id => 52}];
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
  my @value;
  dies_here_ok { $result->each (sub { push @value, $_ }) };
  dies_here_ok { $result->each_as_row (sub { push @value, $_ }) };
  eq_or_diff \@value, [];
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
} # _insert_two_rows_result_all

sub _insert_two_rows_result_all_as_rows : Test(21) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');
  
  my $result = $db->insert ('foo', [{id => 32}, {id => 52}]);
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 2;
  is $result->table_name, 'foo';
  my $values = $result->all_as_rows;
  isa_list_n_ok $values, 2;
  isa_ok $values->[0], 'Dongry::Table::Row';
  is $values->[0]->{db}, $db;
  is $values->[0]->{table_name}, 'foo';
  eq_or_diff $values->[0]->{data}, {id => 32};
  is $values->[0]->{parsed_data}, undef;
  isa_ok $values->[1], 'Dongry::Table::Row';
  is $values->[1]->{db}, $db;
  is $values->[1]->{table_name}, 'foo';
  eq_or_diff $values->[1]->{data}, {id => 52};
  is $values->[1]->{parsed_data}, undef;
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->all };
  my @value;
  dies_here_ok { $result->each_as_row (sub { push @value, $_ }) };
  dies_here_ok { $result->each (sub { push @value, $_ }) };
  eq_or_diff \@value, [];
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
} # _insert_two_rows_result_all_as_rows

sub _insert_two_rows_result_first : Test(11) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');
  
  my $result = $db->insert ('foo', [{id => 32}, {id => 53}]);
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 2;
  is $result->table_name, 'foo';
  my $value = $result->first;
  eq_or_diff $value, {id => 32};
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
  my @value;
  dies_here_ok { $result->each (sub { push @value, $_ }) };
  dies_here_ok { $result->each_as_row (sub { push @value, $_ }) };
  eq_or_diff \@value, [];
} # _insert_two_rows_result_first

sub _insert_two_rows_result_first_as_row : Test(15) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');
  
  my $result = $db->insert ('foo', [{id => 32}, {id => 53}]);
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 2;
  is $result->table_name, 'foo';
  my $value = $result->first_as_row;
  isa_ok $value, 'Dongry::Table::Row';
  is $value->{db}, $db;
  is $value->{table_name}, 'foo';
  eq_or_diff $value->{data}, {id => 32};
  is $value->{parsed_data}, undef;
  dies_here_ok { $result->first_as_row };
  dies_here_ok { $result->first };
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->all };
  my @value;
  dies_here_ok { $result->each_as_row (sub { push @value, $_ }) };
  dies_here_ok { $result->each (sub { push @value, $_ }) };
  eq_or_diff \@value, [];
} # _insert_two_rows_result_first_as_row

sub _insert_rubyish_result_each : Test(4) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');
  
  my $result = $db->insert
      ('foo', List::Rubyish->new ([{id => 32}, {id => 53}]));
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 2;
  is $result->table_name, 'foo';
  my @value;
  $result->each (sub { push @value, $_ });
  eq_or_diff \@value, [{id => 32}, {id => 53}];
} # _insert_rubyish_result_each

sub _insert_rubyish_result_each_as_row : Test(6) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');
  
  my $result = $db->insert
      ('foo', List::Rubyish->new ([{id => 32}, {id => 53}]));
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 2;
  is $result->table_name, 'foo';
  my @value;
  $result->each_as_row (sub { push @value, $_ });
  isa_ok $value[0], 'Dongry::Table::Row';
  isa_ok $value[1], 'Dongry::Table::Row';
  eq_or_diff [map { $_->{data} } @value], [{id => 32}, {id => 53}];
} # _insert_rubyish_result_each_as_row

sub _insert_rubyish_result_all : Test(4) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');
  
  my $result = $db->insert
      ('foo', List::Rubyish->new ([{id => 32}, {id => 53}]));
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 2;
  is $result->table_name, 'foo';
  my $values = $result->all;
  eq_or_diff $values->to_a, [{id => 32}, {id => 53}];
} # _insert_rubyish_result_all

sub _insert_rubyish_result_all_as_rows : Test(6) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');
  
  my $result = $db->insert
      ('foo', List::Rubyish->new ([{id => 32}, {id => 53}]));
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 2;
  is $result->table_name, 'foo';
  my $values = $result->all_as_rows;
  isa_ok $values->[0], 'Dongry::Table::Row';
  isa_ok $values->[1], 'Dongry::Table::Row';
  eq_or_diff $values->map (sub { $_->{data} })->to_a, [{id => 32}, {id => 53}];
} # _insert_rubyish_result_all_as_rows

sub _insert_rubyish_result_first : Test(4) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');
  
  my $result = $db->insert
      ('foo', List::Rubyish->new ([{id => 32}, {id => 53}]));
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 2;
  is $result->table_name, 'foo';
  my $value = $result->first;
  eq_or_diff $value, {id => 32};
} # _insert_rubyish_result_first

sub _insert_rubyish_result_first_as_row : Test(5) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');
  
  my $result = $db->insert
      ('foo', List::Rubyish->new ([{id => 32}, {id => 53}]));
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 2;
  is $result->table_name, 'foo';
  my $value = $result->first_as_row;
  isa_ok $value, 'Dongry::Table::Row';
  eq_or_diff $value->{data}, {id => 32};
} # _insert_rubyish_result_first_as_row

sub _insert_duplicate_error : Test(8) {
  reset_db_set;
  my $dsn = test_dsn 'inserttest1';
  my $invoked = 0;
  my ($onerror_self, %onerror_args);
  my $shortmess;
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}},
       onerror => sub {
         ($onerror_self, %onerror_args) = @_;
         $invoked++;
         $shortmess = Carp::shortmess;
       });

  $db->execute ('create table foo (id int unique key)');
  $db->execute ('insert into foo (id) values (2)');

  my $messline;
  dies_here_ok {
    $messline = __LINE__; $db->insert ('foo', [{id => 2}]);
  };

  is $onerror_self, $db;
  is $onerror_args{source_name}, 'master';
  is $onerror_args{sql}, 'INSERT INTO `foo` (`id`) VALUES (?)';
  #is $onerror_args{text}, q{DBD::mysql::st execute failed: Duplicate entry '2' for key 1}; # MySQL 5.0
  is $onerror_args{text}, q{DBD::mysql::st execute failed: Duplicate entry '2' for key 'id'}; # MySQL 5.5
  is $invoked, 1;
  is $shortmess, ' at ' . __FILE__ . ' line ' . $messline . "\n";
  
  is $db->execute ('select * from foo', undef, source_name => 'master')
      ->row_count, 1;
} # _insert_duplicate_error

sub _insert_ignore_no_duplicate_error : Test(3) {
  reset_db_set;
  my $dsn = test_dsn 'inserttest1';
  my $invoked = 0;
  my ($onerror_self, %onerror_args);
  my $shortmess;
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}},
       onerror => sub {
         ($onerror_self, %onerror_args) = @_;
         $invoked++;
         $shortmess = Carp::shortmess;
       });

  $db->execute ('create table foo (id int unique key, val text)');
  $db->execute ('insert into foo (id, val) values (2, "abc")');

  is $db->insert ('foo', [{id => 2, val => 'xyz'}], duplicate => 'ignore')
      ->row_count, 0;
  is $db->{last_sql}, 'INSERT IGNORE INTO `foo` (`id`, `val`) VALUES (?, ?)';
  
  eq_or_diff $db->execute ('select * from foo', undef, source_name => 'master')
      ->all->to_a, [{id => 2, val => 'abc'}];
} # _insert_ignore_no_duplicate_error

sub _insert_replace_no_duplicate_error : Test(3) {
  reset_db_set;
  my $dsn = test_dsn 'inserttest1';
  my $invoked = 0;
  my ($onerror_self, %onerror_args);
  my $shortmess;
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}},
       onerror => sub {
         ($onerror_self, %onerror_args) = @_;
         $invoked++;
         $shortmess = Carp::shortmess;
       });

  $db->execute ('create table foo (id int unique key, val text)');
  $db->execute ('insert into foo (id, val) values (2, "abc")');

  is $db->insert ('foo', [{id => 2, val => 'xyz'}], duplicate => 'replace')
      ->row_count, 2;
  is $db->{last_sql}, 'REPLACE INTO `foo` (`id`, `val`) VALUES (?, ?)';
  
  eq_or_diff $db->execute ('select * from foo', undef, source_name => 'master')
      ->all->to_a, [{id => 2, val => 'xyz'}];
} # _insert_replace_no_duplicate_error

sub _insert_column_error : Test(8) {
  reset_db_set;
  my $dsn = test_dsn 'inserttest1';
  my $invoked = 0;
  my ($onerror_self, %onerror_args);
  my $shortmess;
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}},
       onerror => sub {
         ($onerror_self, %onerror_args) = @_;
         $invoked++;
         $shortmess = Carp::shortmess;
       });

  $db->execute ('create table foo (id int unique key)');

  my $messline;
  dies_here_ok {
    $messline = __LINE__; $db->execute ('insert into foo (id2) values (2)');
  };

  is $onerror_self, $db;
  is $onerror_args{source_name}, 'master';
  is $onerror_args{sql}, 'insert into foo (id2) values (2)';
  ok $onerror_args{text};
  is $invoked, 1;
  is $shortmess, ' at ' . __FILE__ . ' line ' . $messline . "\n";
  
  is $db->execute ('select * from foo', undef, source_name => 'master')
      ->row_count, 0;
} # _insert_column_error

sub _insert_stupid_table_name : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'select1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table `ho``ge``_(a)` (id int)');

  $db->insert ('ho`ge`_(a)', [{id => 1}]);

  is $db->execute ('select * from `ho``ge``_(a)`', undef,
                   source_name => 'master')->row_count, 1;
} # _insert_stupid_table_name

sub _insert_stupid_column_name : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'select1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table hoge (`ho``ge``_\\(a)` int)');

  $db->insert ('hoge', [{'ho`ge`_\\(a)' => 1}]);

  is $db->execute ('select * from hoge where `ho``ge``_\\(a)` = 1', undef,
                   source_name => 'master')->row_count, 1;
} # _insert_stupid_column_name

sub _insert_utf8_flagged_string : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'inserttest1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});

  $db->execute ('create table foo (id int unique key, val text)');

  eq_or_diff $db->insert ('foo', [{id => 2, val => "\x{5000}\x{6000}"}])
      ->all->to_a, [{id => 2, val => "\x{5000}\x{6000}"}];
  
  eq_or_diff $db->execute ('select * from foo', undef, source_name => 'master')
      ->all->to_a, [{id => 2, val => encode 'utf-8', "\x{5000}\x{6000}"}];
} # _insert_utf8_flagged_string

sub _insert_utf8_unflagged_string : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'inserttest1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});

  $db->execute ('create table foo (id int unique key, val text)');

  eq_or_diff $db->insert ('foo', [{id => 2,
                                   val => encode 'utf-8', "\x{5000}\x{6000}"}])
      ->all->to_a, [{id => 2, val => encode 'utf-8', "\x{5000}\x{6000}"}];
  
  eq_or_diff $db->execute ('select * from foo', undef, source_name => 'master')
      ->all->to_a, [{id => 2, val => encode 'utf-8', "\x{5000}\x{6000}"}];
} # _insert_utf8_unflagged_string

sub _insert_latin1_string : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'inserttest1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});

  $db->execute ('create table foo (id int unique key, val text)');

  eq_or_diff $db->insert ('foo', [{id => 2, val => "\x{a5}\x{81}\x{d5}"}])
      ->all->to_a, [{id => 2, val => "\x{a5}\x{81}\x{d5}"}];
  
  eq_or_diff $db->execute ('select * from foo', undef, source_name => 'master')
      ->all->to_a, [{id => 2, val => encode 'latin1', "\x{a5}\x{81}\x{d5}"}];
} # _insert_latin1_string

sub _insert_utf8_flagged_table : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'inserttest1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});

  $db->execute
      ((encode 'utf-8',
        qq{create table `\x{5000}\x{6000}` (id int unique key, val text)}));

  my $result = $db->insert ("\x{5000}\x{6000}", [{id => 2, val => "abc"}]);
  is $result->table_name, "\x{5000}\x{6000}";
  
  eq_or_diff $db->execute
      ((encode 'utf-8', qq{select * from `\x{5000}\x{6000}`}), undef,
       source_name => 'master')->all->to_a,
           [{id => 2, val => "abc"}];
} # _insert_utf8_flagged_table

sub _insert_utf8_unflagged_table : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'inserttest1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});

  $db->execute
      ((encode 'utf-8',
        qq{create table `\x{5000}\x{6000}` (id int unique key, val text)}));

  my $result = $db->insert ((encode 'utf-8', "\x{5000}\x{6000}"),
                            [{id => 2, val => "abc"}]);
  
  eq_or_diff $db->execute
      ((encode 'utf-8', qq{select * from `\x{5000}\x{6000}`}), undef,
       source_name => 'master')->all->to_a,
           [{id => 2, val => "abc"}];
} # _insert_utf8_unflagged_table

sub _insert_utf8_flagged_column : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'inserttest1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});

  $db->execute
      ((encode 'utf-8',
        qq{create table foo (id int, `\x{5000}\x{6000}` text)}));

  my $result = $db->insert ('foo', [{id => 2, "\x{5000}\x{6000}" => 'ho'}]);
  
  eq_or_diff $db->execute
      ((encode 'utf-8', qq{select * from foo}), undef,
       source_name => 'master')->all->to_a,
           [{id => 2, (encode 'utf-8', "\x{5000}\x{6000}") => "ho"}];
} # _insert_utf8_flagged_table

sub _insert_utf8_unflagged_column : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'inserttest1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});

  $db->execute
      ((encode 'utf-8',
        qq{create table foo (id int, `\x{5000}\x{6000}` text)}));

  my $result = $db->insert
      ('foo', [{id => 2, (encode 'utf-8', "\x{5000}\x{6000}") => 'ho'}]);

  eq_or_diff $db->execute
      ((encode 'utf-8', qq{select * from foo}), undef,
       source_name => 'master')->all->to_a,
           [{id => 2, (encode 'utf-8', "\x{5000}\x{6000}") => "ho"}];
} # _insert_utf8_unflagged_table

sub _insert_object : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'inserttest1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});

  $db->execute ('create table foo (id int unique key, val text)');

  my $obj = file (__FILE__);
  eq_or_diff $db->insert ('foo', [{id => 2, val => $obj}])
      ->all->to_a, [{id => 2, val => $obj}];
  
  eq_or_diff $db->execute ('select * from foo', undef, source_name => 'master')
      ->all->to_a, [{id => 2, val => '' . $obj}];
} # _insert_object

sub _insert_with_default : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'select1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute
      ('create table hoge (v1 int default 4, v2 int default 5, v3 int)');

  $db->insert ('hoge', [{v1 => 12}, {v2 => 44}, {v3 => 444}]);
  is $db->{last_sql},
      'INSERT INTO `hoge` (`v1`, `v2`, `v3`) VALUES '.
      '(?, DEFAULT, DEFAULT), (DEFAULT, ?, DEFAULT), (DEFAULT, DEFAULT, ?)';

  eq_or_diff $db->execute ('select * from hoge order by v1 desc, v2 desc',
                           undef,
                           source_name => 'master')->all->to_a,
             [{v1 => 12, v2 => 5, v3 => undef},
              {v1 => 4, v2 => 44, v3 => undef},
              {v1 => 4, v2 => 5, v3 => 444}];
} # _insert_with_default

sub _insert_duplicate_update_found : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'select1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table hoge (v1 int unique key, v2 int, v3 blob)');
  $db->execute ('insert into hoge (v1, v2) values (1, 2112)');
  $db->execute ('insert into hoge (v1, v2) values (2, 422)');

  my $result = $db->insert
      ('hoge', [{v1 => 1}], duplicate => {v3 => 'updated'});
  is $result->row_count, 3; # 2 in MySQL 5.0; 3 in MySQL 5.5.

  eq_or_diff $db->execute ('select * from hoge order by v1 asc', undef,
                           source_name => 'master')->all->to_a,
             [{v1 => 1, v2 => 2112, v3 => 'updated'},
              {v1 => 2, v2 => 422, v3 => undef}];
} # _insert_duplicate_update_found

sub _insert_duplicate_update_not_found : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'select1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table hoge (v1 int unique key, v2 int, v3 blob)');
  $db->execute ('insert into hoge (v1, v2) values (1, 2112)');
  $db->execute ('insert into hoge (v1, v2) values (2, 422)');

  my $result = $db->insert
      ('hoge', [{v1 => 3}], duplicate => {v3 => 'updated'});
  is $result->row_count, 1;

  eq_or_diff $db->execute ('select * from hoge order by v1 asc', undef,
                           source_name => 'master')->all->to_a,
             [{v1 => 1, v2 => 2112, v3 => undef},
              {v1 => 2, v2 => 422, v3 => undef},
              {v1 => 3, v2 => undef, v3 => undef}];
} # _insert_duplicate_update_not_found

sub _insert_duplicate_update_found_not_sql : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'select1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table hoge (v1 int unique key, v2 int, v3 blob)');
  $db->execute ('insert into hoge (v1, v2) values (1, 2112)');
  $db->execute ('insert into hoge (v1, v2) values (2, 422)');

  my $v = \'values(v2)+3';
  my $result = $db->insert
      ('hoge', [{v1 => 1, v2 => 321}], duplicate => {v3 => $v});
  is $result->row_count, 3; # 2 in MySQL 5.0; 3 in MySQL 5.5.

  eq_or_diff $db->execute ('select * from hoge order by v1 asc', undef,
                           source_name => 'master')->all->to_a,
             [{v1 => 1, v2 => 2112, v3 => '' . $v},
              {v1 => 2, v2 => 422, v3 => undef}];
} # _insert_duplicate_update_found_not_sql

sub _insert_duplicate_update_found_sql : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'select1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table hoge (v1 int unique key, v2 int, v3 blob)');
  $db->execute ('insert into hoge (v1, v2) values (1, 2112)');
  $db->execute ('insert into hoge (v1, v2) values (2, 422)');

  my $result = $db->insert
      ('hoge', [{v1 => 1, v2 => 321}],
       duplicate => {v3 => $db->bare_sql_fragment ('values(v2)+3')});
  is $result->row_count, 3; # 2 in MySQL 5.0; 3 in MySQL 5.5.

  eq_or_diff $db->execute ('select * from hoge order by v1 asc', undef,
                           source_name => 'master')->all->to_a,
             [{v1 => 1, v2 => 2112, v3 => 321 + 3},
              {v1 => 2, v2 => 422, v3 => undef}];
} # _insert_duplicate_update_found_sql

sub _insert_duplicate_update_found_sql_2 : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'select1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table hoge (v1 int unique key, v2 int, v3 blob)');
  $db->execute ('insert into hoge (v1, v2) values (1, 2112)');
  $db->execute ('insert into hoge (v1, v2) values (2, 422)');

  my $result = $db->insert
      ('hoge', [{v1 => 1, v2 => 321}],
       duplicate => {v2 => $db->bare_sql_fragment ('v2 + 2'),
                     v3 => $db->bare_sql_fragment ('values(v2)+3')});
  is $result->row_count, 3; # 2 in MySQL 5.0; 3 in MySQL 5.5.

  eq_or_diff $db->execute ('select * from hoge order by v1 asc', undef,
                           source_name => 'master')->all->to_a,
             [{v1 => 1, v2 => 2112 + 2, v3 => 321 + 3},
              {v1 => 2, v2 => 422, v3 => undef}];
} # _insert_duplicate_update_found_sql_2

sub _insert_duplicate_update_found_stupid_column : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'select1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table hoge (v1 int unique key, `22``` int, v3 blob)');
  $db->execute ('insert into hoge (v1, `22```) values (1, 2112)');
  $db->execute ('insert into hoge (v1, `22```) values (2, 422)');

  my $result = $db->insert
      ('hoge', [{v1 => 1}], duplicate => {'22`' => 1, v3 => 'updated'});
  is $result->row_count, 3; # 2 in MySQL 5.0; 3 in MySQL 5.5.

  eq_or_diff $db->execute ('select * from hoge order by v1 asc', undef,
                           source_name => 'master')->all->to_a,
             [{v1 => 1, '22`' => 1, v3 => 'updated'},
              {v1 => 2, '22`' => 422, v3 => undef}];
} # _insert_duplicate_update_found_stupid_column

sub _insert_duplicate_update_found_utf8_flagged_value : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'select1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table hoge (v1 int unique key, v2 int, v3 blob)');
  $db->execute ('insert into hoge (v1, v2) values (1, 2112)');
  $db->execute ('insert into hoge (v1, v2) values (2, 422)');

  my $result = $db->insert
      ('hoge', [{v1 => 1}], duplicate => {v3 => "\x{5000}"});
  is $result->row_count, 3; # 2 in MySQL 5.0; 3 in MySQL 5.5.

  eq_or_diff $db->execute ('select * from hoge order by v1 asc', undef,
                           source_name => 'master')->all->to_a,
             [{v1 => 1, v2 => 2112, v3 => encode 'utf-8', "\x{5000}"},
              {v1 => 2, v2 => 422, v3 => undef}];
} # _insert_duplicate_update_found_utf8_flagged_value

sub _insert_bare_fragment : Test(1) {
  my $db = new_db;
  $db->execute ('create table foo (id int)');

  $db->insert ('foo', [{id => $db->bare_sql_fragment ('120 * 21')}]);

  my $data = $db->select ('foo', {id => {-not => undef}})->first;
  is $data->{id}, 120 * 21;
} # _insert_bare_fragment

sub _insert_bare_fragment_bad : Test(2) {
  my $db = new_db;
  $db->execute ('create table foo (id int)');

  dies_here_ok {
    $db->insert ('foo', [{id => $db->bare_sql_fragment ('120 * ')}]);
  };

  my $data = $db->select ('foo', {id => {-not => undef}});
  is $data->row_count, 0;
} # _insert_bare_fragment_bad

sub _insert_cb : Test(8) {
  my $db = new_db;
  $db->execute ('create table foo (id int)');

  my $result;
  $db->insert ('foo', [{id => 12}, {id => 12}], cb => sub {
    is $_[0], $db;
    $result = $_[1];
  });

  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  ng $result->error_text;
  ng $result->error_sql;
  is $result->row_count, 2;
  eq_or_diff $result->all->to_a, [{id => 12}, {id => 12}];
} # _insert_cb

sub _insert_cb_return : Test(10) {
  my $db = new_db;
  $db->execute ('create table foo (id int)');

  my $result;
  my $result2 = $db->insert ('foo', [{id => 12}, {id => 12}], cb => sub {
    is $_[0], $db;
    $result = $_[1];
  });

  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  ng $result->error_text;
  ng $result->error_sql;
  is $result->row_count, 2;
  eq_or_diff $result->all->to_a, [{id => 12}, {id => 12}];
  is $result2, $result;
  is $result2->row_count, $result->row_count;
} # _insert_cb_return

sub _insert_cb_exception : Test(1) {
  my $db = new_db;
  $db->execute ('create table foo (id int)');

  eval {
    $db->insert ('foo', [{id => 12}, {id => 12}], cb => sub {
      die 'abbc ';
    });
  };
  is $@, 'abbc  at ' . __FILE__ . ' line ' . (__LINE__ - 3) . ".\n";
} # _insert_cb_exception

sub _insert_cb_exception_croak : Test(1) {
  my $db = new_db;
  $db->execute ('create table foo (id int)');

  eval {
    $db->insert ('foo', [{id => 12}, {id => 12}], cb => sub {
      Carp::croak 'abbc ';
    });
  };
  is $@, 'abbc  at ' . __FILE__ . ' line ' . (__LINE__ - 2) . "\n";
} # _insert_cb_exception_croak

sub _insert_cb_onerror : Test(2) {
  my $db = new_db;
  $db->execute ('create table foo (id int)');

  my $invoked;
  dies_here_ok {
    $db->insert ('foo', [{id => 12}, {notid => 12}], cb => sub {
      $invoked++;
    });
  };
  ng $invoked;
} # _insert_cb_onerror

__PACKAGE__->runtests;

1;

=head1 LICENSE

Copyright 2011 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
