package test::Dongry::Database::insert;
use strict;
use warnings;
use Path::Class;
use lib file (__FILE__)->dir->parent->subdir ('lib')->stringify;
use Test::Dongry;
use base qw(Test::Class);
use Dongry::Database;
use Encode;

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
  
  dies_ok { $db->insert ('foo', []) };

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
  
  dies_ok { $db->insert ('foo', [{id => 31}]) };

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
  
  is $db1->execute ('select * from foo', [],
                    source_name => 'master')->row_count, 0;
  is $db2->execute ('select * from foo', [],
                    source_name => 'master')->row_count, 0;
  is $db3->execute ('select * from foo', [],
                    source_name => 'master')->row_count, 1;
} # _insert_source_heavy

sub _insert_source_not_defined : Test(1) {
  my $db = Dongry::Database->new;
  dies_ok { $db->insert ('foo', [{id => 444}]) };
} # _insert_source_not_defined

sub _insert_source_not_found : Test(1) {
  my $db = Dongry::Database->new
      (sources => {hoge => {dsn => 'foo'}});
  dies_ok { $db->insert ('foo', [{id => 444}], source_name => 'fuga') };
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
  
  dies_ok { $db->insert ({id => 31}, source_name => 'master') };
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
  dies_ok { $result->each (sub { push @value, $_ }) };
  dies_ok { $result->each_as_row (sub { push @value, $_ }) };
  eq_or_diff \@value, [];
  dies_ok { $result->all };
  dies_ok { $result->all_as_rows };
  dies_ok { $result->first };
  dies_ok { $result->first_as_row };
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
  dies_ok { $result->each_as_row (sub { push @value, $_ }) };
  dies_ok { $result->each (sub { push @value, $_ }) };
  eq_or_diff \@value, [];
  dies_ok { $result->all };
  dies_ok { $result->all_as_rows };
  dies_ok { $result->first };
  dies_ok { $result->first_as_row };
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
  dies_ok { $result->all };
  dies_ok { $result->all_as_rows };
  my @value;
  dies_ok { $result->each (sub { push @value, $_ }) };
  dies_ok { $result->each_as_row (sub { push @value, $_ }) };
  eq_or_diff \@value, [];
  dies_ok { $result->first };
  dies_ok { $result->first_as_row };
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
  dies_ok { $result->all_as_rows };
  dies_ok { $result->all };
  my @value;
  dies_ok { $result->each_as_row (sub { push @value, $_ }) };
  dies_ok { $result->each (sub { push @value, $_ }) };
  eq_or_diff \@value, [];
  dies_ok { $result->first };
  dies_ok { $result->first_as_row };
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
  dies_ok { $result->first };
  dies_ok { $result->first_as_row };
  dies_ok { $result->all };
  dies_ok { $result->all_as_rows };
  my @value;
  dies_ok { $result->each (sub { push @value, $_ }) };
  dies_ok { $result->each_as_row (sub { push @value, $_ }) };
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
  dies_ok { $result->first_as_row };
  dies_ok { $result->first };
  dies_ok { $result->all_as_rows };
  dies_ok { $result->all };
  my @value;
  dies_ok { $result->each_as_row (sub { push @value, $_ }) };
  dies_ok { $result->each (sub { push @value, $_ }) };
  eq_or_diff \@value, [];
} # _insert_a_row_result_first_as_row

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
  dies_ok { $result->each (sub { push @value, $_ }) };
  dies_ok { $result->each_as_row (sub { push @value, $_ }) };
  eq_or_diff \@value, [];
  dies_ok { $result->all };
  dies_ok { $result->all_as_rows };
  dies_ok { $result->first };
  dies_ok { $result->first_as_row };
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
  dies_ok { $result->each_as_row (sub { push @value, $_ }) };
  dies_ok { $result->each (sub { push @value, $_ }) };
  eq_or_diff \@value, [];
  dies_ok { $result->all };
  dies_ok { $result->all_as_rows };
  dies_ok { $result->first };
  dies_ok { $result->first_as_row };
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
  dies_ok { $result->all };
  dies_ok { $result->all_as_rows };
  my @value;
  dies_ok { $result->each (sub { push @value, $_ }) };
  dies_ok { $result->each_as_row (sub { push @value, $_ }) };
  eq_or_diff \@value, [];
  dies_ok { $result->first };
  dies_ok { $result->first_as_row };
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
  dies_ok { $result->all_as_rows };
  dies_ok { $result->all };
  my @value;
  dies_ok { $result->each_as_row (sub { push @value, $_ }) };
  dies_ok { $result->each (sub { push @value, $_ }) };
  eq_or_diff \@value, [];
  dies_ok { $result->first };
  dies_ok { $result->first_as_row };
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
  dies_ok { $result->first };
  dies_ok { $result->first_as_row };
  dies_ok { $result->all };
  dies_ok { $result->all_as_rows };
  my @value;
  dies_ok { $result->each (sub { push @value, $_ }) };
  dies_ok { $result->each_as_row (sub { push @value, $_ }) };
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
  dies_ok { $result->first_as_row };
  dies_ok { $result->first };
  dies_ok { $result->all_as_rows };
  dies_ok { $result->all };
  my @value;
  dies_ok { $result->each_as_row (sub { push @value, $_ }) };
  dies_ok { $result->each (sub { push @value, $_ }) };
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
  dies_ok {
    $messline = __LINE__; $db->execute ('insert into foo (id) values (2)');
  };

  is $onerror_self, $db;
  is $onerror_args{source_name}, 'master';
  is $onerror_args{sql}, 'insert into foo (id) values (2)';
  ok $onerror_args{text};
  is $invoked, 1;
  is $shortmess, ' at ' . __FILE__ . ' line ' . $messline . "\n";
  
  is $db->execute ('select * from foo', undef, source_name => 'master')
      ->row_count, 1;
} # _insert_duplicate_error

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
  dies_ok {
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

sub _last_insert_id_unknown : Test(1) {
  my $db = Dongry::Database->new;
  is $db->last_insert_id, undef;
} # _last_insert_id_unknown

sub _last_insert_id_unknown_2 : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'select1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 1}});
  $db->execute ('create table hoge (id int)', undef, source_name => 'default');
  $db->execute ('select * from hoge');
  is $db->last_insert_id, undef;
} # _last_insert_id_unknown_2

sub _last_insert_id_inserted : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'select1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table hoge (id int)');
  $db->execute ('insert into hoge (id) values (2)');
  is $db->last_insert_id, 0;
} # _last_insert_id_inserted

sub _last_insert_id_inserted_pk : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'select1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table hoge (id int primary key)');
  $db->execute ('insert into hoge (id) values (2)');
  is $db->last_insert_id, 0;
} # _last_insert_id_inserted_pk

sub _last_insert_id_inserted_pk_auto_increment : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'select1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table hoge (id int primary key auto_increment)');
  $db->execute ('insert into hoge (id) values (2)');
  is $db->last_insert_id, 2;
} # _last_insert_id_inserted_pk_auto_increment

sub _last_insert_id_inserted_multiple : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'select1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table hoge (id int primary key auto_increment)');
  $db->execute ('insert into hoge (id) values (2), (3)');
  ok $db->last_insert_id;
} # _last_insert_id_inserted_multiple

sub _last_insert_id_inserted_multiple_statements : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'select1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table hoge (id int primary key auto_increment)');
  $db->execute ('insert into hoge (id) values (2)');
  $db->execute ('insert into hoge (id) values (3)');
  is $db->last_insert_id, 3;
} # _last_insert_id_inserted_multiple_statements

__PACKAGE__->runtests;

1;
