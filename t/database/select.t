package test::Dongry::Database::select;
use strict;
use warnings;
use Path::Class;
use lib file (__FILE__)->dir->parent->subdir ('lib')->stringify;
use Test::Dongry;
use base qw(Test::Class);
use Dongry::Database;
use Encode;
use DateTime;

sub _select_nothing_each : Test(11) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');
  
  my $result = $db->select ('foo', {id => 1243, v1 => "hoge", v2 => undef});
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 0;
  is $result->table_name, 'foo';
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
} # _select_nothing_each

sub _select_nothing_each_as_row : Test(11) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');
  
  my $result = $db->select ('foo', {id => 1243, v1 => "hoge", v2 => undef});
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 0;
  is $result->table_name, 'foo';
  my $invoked = 0;
  $result->each_as_row (sub { $invoked++ });
  is $invoked, 0;
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  dies_here_ok { $result->each (sub { $invoked++ }) };
  is $invoked, 0;
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
} # _select_nothing_each_as_row

sub _select_nothing_first : Test(11) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');
  
  my $result = $db->select ('foo', {id => 1243, v1 => "hoge", v2 => undef});
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 0;
  is $result->table_name, 'foo';
  is $result->first, undef;
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
  my $invoked = 0;
  dies_here_ok { $result->each (sub { $invoked++ }) };
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 0;
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
} # _select_nothing_first

sub _select_nothing_first_as_row : Test(11) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');
  
  my $result = $db->select ('foo', {id => 1243, v1 => "hoge", v2 => undef});
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 0;
  is $result->table_name, 'foo';
  is $result->first_as_row, undef;
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
  my $invoked = 0;
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  dies_here_ok { $result->each (sub { $invoked++ }) };
  is $invoked, 0;
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
} # _select_nothing_first_as_row

sub _select_nothing_all : Test(11) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');
  
  my $result = $db->select ('foo', {id => 1243, v1 => "hoge", v2 => undef});
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 0;
  is $result->table_name, 'foo';
  isa_list_n_ok $result->all, 0;
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
  my $invoked = 0;
  dies_here_ok { $result->each (sub { $invoked++ }) };
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 0;
} # _select_nothing_all

sub _select_nothing_all_as_rows : Test(11) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');
  
  my $result = $db->select ('foo', {id => 1243, v1 => "hoge", v2 => undef});
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 0;
  is $result->table_name, 'foo';
  isa_list_n_ok $result->all_as_rows, 0;
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
  my $invoked = 0;
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  dies_here_ok { $result->each (sub { $invoked++ }) };
  is $invoked, 0;
} # _select_nothing_first_as_row

sub _select_a_row_each : Test(12) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');
  $db->execute ('insert into foo (id, v1, v2) values (12, "abc", 322)');
  
  my $result = $db->select ('foo', {id => 12});
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 1;
  is $result->table_name, 'foo';
  my $invoked = 0;
  my @value;
  $result->each (sub { push @value, $_; $invoked++ });
  is $invoked, 1;
  eq_or_diff \@value, [{id => 12, v1 => 'abc', v2 => '322'}];
  dies_here_ok { $result->each (sub { $invoked++ }) };
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 1;
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
} # _select_a_row_each

sub _select_a_row_each_as_row : Test(15) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');
  $db->execute ('insert into foo (id, v1, v2) values (12, "abc", 322)');
  
  my $result = $db->select ('foo', {id => 12});
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 1;
  is $result->table_name, 'foo';
  my $invoked = 0;
  my @value;
  $result->each_as_row (sub { push @value, $_; $invoked++ });
  is $invoked, 1;
  isa_ok $value[0], 'Dongry::Table::Row';
  is $value[0]->{db}, $db;
  is $value[0]->{table_name}, 'foo';
  eq_or_diff $value[0]->{data}, {id => 12, v1 => 'abc', v2 => '322'};
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  dies_here_ok { $result->each (sub { $invoked++ }) };
  is $invoked, 1;
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
} # _select_a_row_each_as_row

sub _select_a_row_first : Test(12) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');
  $db->execute ('insert into foo (id, v1, v2) values (12, "abc", 322)');
  
  my $result = $db->select ('foo', {id => 12});
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 1;
  is $result->table_name, 'foo';
  my $invoked = 0;
  eq_or_diff $result->first, {id => 12, v1 => 'abc', v2 => '322'};
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
  dies_here_ok { $result->each (sub { $invoked++ }) };
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 0;
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
} # _select_a_row_first

sub _select_a_row_first_as_row : Test(14) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');
  $db->execute ('insert into foo (id, v1, v2) values (12, "abc", 322)');
  
  my $result = $db->select ('foo', {id => 12});
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 1;
  is $result->table_name, 'foo';
  my $invoked = 0;
  my $value = $result->first_as_row;
  isa_ok $value, 'Dongry::Table::Row';
  is $value->{db}, $db;
  is $value->{table_name}, 'foo';
  eq_or_diff $value->{data}, {id => 12, v1 => 'abc', v2 => '322'};
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  dies_here_ok { $result->each (sub { $invoked++ }) };
  is $invoked, 0;
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
} # _select_a_row_first_as_row

sub _select_a_row_all : Test(12) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');
  $db->execute ('insert into foo (id, v1, v2) values (12, "abc", 322)');
  
  my $result = $db->select ('foo', {id => 12});
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 1;
  is $result->table_name, 'foo';
  my $invoked = 0;
  my $list = $result->all;
  isa_list_n_ok $list, 1;
  eq_or_diff $list->to_a, [{id => 12, v1 => 'abc', v2 => '322'}];
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
  dies_here_ok { $result->each (sub { $invoked++ }) };
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 0;
} # _select_a_row_all

sub _select_a_row_all_as_rows : Test(15) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');
  $db->execute ('insert into foo (id, v1, v2) values (12, "abc", 322)');
  
  my $result = $db->select ('foo', {id => 12});
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 1;
  is $result->table_name, 'foo';
  my $invoked = 0;
  my $list = $result->all_as_rows;
  isa_list_n_ok $list, 1;
  my $values = $list->to_a;
  isa_ok $values->[0], 'Dongry::Table::Row';
  is $values->[0]->{db}, $db;
  is $values->[0]->{table_name}, 'foo';
  eq_or_diff $values->[0]->{data}, {id => 12, v1 => 'abc', v2 => '322'};
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  dies_here_ok { $result->each (sub { $invoked++ }) };
  is $invoked, 0;
} # _select_a_row_all_as_rows

sub _select_a_row_each_zero : Test(12) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table `0` (id int, v1 text, v2 text)');
  $db->execute ('insert into `0` (id, v1, v2) values (12, "abc", 322)');
  
  my $result = $db->select ('0', {id => 12});
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 1;
  is $result->table_name, '0';
  my $invoked = 0;
  my @value;
  $result->each (sub { push @value, $_; $invoked++ });
  is $invoked, 1;
  eq_or_diff \@value, [{id => 12, v1 => 'abc', v2 => '322'}];
  dies_here_ok { $result->each (sub { $invoked++ }) };
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 1;
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
} # _select_a_row_each_zero

sub _select_a_row_each_as_row_zero : Test(15) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table `0` (id int, v1 text, v2 text)');
  $db->execute ('insert into `0` (id, v1, v2) values (12, "abc", 322)');
  
  my $result = $db->select ('0', {id => 12});
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 1;
  is $result->table_name, '0';
  my $invoked = 0;
  my @value;
  $result->each_as_row (sub { push @value, $_; $invoked++ });
  is $invoked, 1;
  isa_ok $value[0], 'Dongry::Table::Row';
  is $value[0]->{db}, $db;
  is $value[0]->{table_name}, '0';
  eq_or_diff $value[0]->{data}, {id => 12, v1 => 'abc', v2 => '322'};
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  dies_here_ok { $result->each (sub { $invoked++ }) };
  is $invoked, 1;
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
} # _select_a_row_each_as_row_zero

sub _select_a_row_first_zero : Test(12) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table `0` (id int, v1 text, v2 text)');
  $db->execute ('insert into `0` (id, v1, v2) values (12, "abc", 322)');
  
  my $result = $db->select ('0', {id => 12});
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 1;
  is $result->table_name, '0';
  my $invoked = 0;
  eq_or_diff $result->first, {id => 12, v1 => 'abc', v2 => '322'};
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
  dies_here_ok { $result->each (sub { $invoked++ }) };
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 0;
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
} # _select_a_row_first_zero

sub _select_a_row_first_as_row_zero : Test(14) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');
  $db->execute ('insert into foo (id, v1, v2) values (12, "abc", 322)');
  
  my $result = $db->select ('foo', {id => 12});
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 1;
  is $result->table_name, 'foo';
  my $invoked = 0;
  my $value = $result->first_as_row;
  isa_ok $value, 'Dongry::Table::Row';
  is $value->{db}, $db;
  is $value->{table_name}, 'foo';
  eq_or_diff $value->{data}, {id => 12, v1 => 'abc', v2 => '322'};
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  dies_here_ok { $result->each (sub { $invoked++ }) };
  is $invoked, 0;
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
} # _select_a_row_first_as_row_zero

sub _select_a_row_all_zero : Test(12) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');
  $db->execute ('insert into foo (id, v1, v2) values (12, "abc", 322)');
  
  my $result = $db->select ('foo', {id => 12});
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 1;
  is $result->table_name, 'foo';
  my $invoked = 0;
  my $list = $result->all;
  isa_list_n_ok $list, 1;
  eq_or_diff $list->to_a, [{id => 12, v1 => 'abc', v2 => '322'}];
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
  dies_here_ok { $result->each (sub { $invoked++ }) };
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 0;
} # _select_a_row_all

sub _select_a_row_all_as_rows : Test(15) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');
  $db->execute ('insert into foo (id, v1, v2) values (12, "abc", 322)');
  
  my $result = $db->select ('foo', {id => 12});
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 1;
  is $result->table_name, 'foo';
  my $invoked = 0;
  my $list = $result->all_as_rows;
  isa_list_n_ok $list, 1;
  my $values = $list->to_a;
  isa_ok $values->[0], 'Dongry::Table::Row';
  is $values->[0]->{db}, $db;
  is $values->[0]->{table_name}, 'foo';
  eq_or_diff $values->[0]->{data}, {id => 12, v1 => 'abc', v2 => '322'};
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  dies_here_ok { $result->each (sub { $invoked++ }) };
  is $invoked, 0;
} # _select_a_row_all_as_rows_zero

sub _select_multiple_rows_each : Test(12) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');
  $db->execute ('insert into foo (id, v1, v2) values (12, "abc", 322)');
  $db->execute ('insert into foo (id, v1, v2) values (23, NULL, "xyxa")');
  
  my $result = $db->select ('foo', {id => {-in => [12, 23]}});
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 2;
  is $result->table_name, 'foo';
  my $invoked = 0;
  my @value;
  $result->each (sub { push @value, $_; $invoked++ });
  is $invoked, 2;
  eq_or_diff [sort { $a->{id} <=> $b->{id} } @value],
             [{id => 12, v1 => 'abc', v2 => '322'},
              {id => 23, v1 => undef, v2 => 'xyxa'}];
  dies_here_ok { $result->each (sub { $invoked++ }) };
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 2;
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
} # _select_multiple_rows_each

sub _select_multiple_rows_each_as_row : Test(19) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');
  $db->execute ('insert into foo (id, v1, v2) values (12, "abc", 322)');
  $db->execute ('insert into foo (id, v1, v2) values (23, NULL, "xyxa")');
  
  my $result = $db->select ('foo', {id => {-in => [12, 23]}});
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 2;
  is $result->table_name, 'foo';
  my $invoked = 0;
  my @value;
  $result->each_as_row (sub { push @value, $_; $invoked++ });
  is $invoked, 2;
  @value = sort { $a->{data}->{id} <=> $b->{data}->{id} } @value;
  isa_ok $value[0], 'Dongry::Table::Row';
  is $value[0]->{db}, $db;
  is $value[0]->{table_name}, 'foo';
  eq_or_diff $value[0]->{data}, {id => 12, v1 => 'abc', v2 => '322'};
  isa_ok $value[1], 'Dongry::Table::Row';
  is $value[1]->{db}, $db;
  is $value[1]->{table_name}, 'foo';
  eq_or_diff $value[1]->{data}, {id => 23, v1 => undef, v2 => 'xyxa'};
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  dies_here_ok { $result->each (sub { $invoked++ }) };
  is $invoked, 2;
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
} # _select_multiple_rows_each_as_row

sub _select_multiple_rows_first : Test(12) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');
  $db->execute ('insert into foo (id, v1, v2) values (12, "abc", 322)');
  $db->execute ('insert into foo (id, v1, v2) values (23, NULL, "xyxa")');
  
  my $result = $db->select ('foo', {id => {-in => [12, 23]}});
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 2;
  is $result->table_name, 'foo';
  my $invoked = 0;
  my $value = $result->first;
  eq_or_diff $value,
      $value->{id} == 12 ? {id => 12, v1 => 'abc', v2 => '322'}
                         : {id => 23, v1 => undef, v2 => 'xyxa'};
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
  dies_here_ok { $result->each (sub { $invoked++ }) };
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 0;
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
} # _select_multiple_rows_first

sub _select_multiple_rows_first_as_row : Test(14) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');
  $db->execute ('insert into foo (id, v1, v2) values (12, "abc", 322)');
  $db->execute ('insert into foo (id, v1, v2) values (23, NULL, "xyxa")');
  
  my $result = $db->select ('foo', {id => {-in => [12, 23]}});
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 2;
  is $result->table_name, 'foo';
  my $invoked = 0;
  my $value = $result->first_as_row;
  isa_ok $value, 'Dongry::Table::Row';
  is $value->{db}, $db;
  is $value->{table_name}, 'foo';
  eq_or_diff $value->{data},
      $value->{data}->{id} == 12 ? {id => 12, v1 => 'abc', v2 => '322'}
                                 : {id => 23, v1 => undef, v2 => 'xyxa'};
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  dies_here_ok { $result->each (sub { $invoked++ }) };
  is $invoked, 0;
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
} # _select_multiple_rows_first_as_row

sub _select_multiple_rows_all : Test(12) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');
  $db->execute ('insert into foo (id, v1, v2) values (12, "abc", 322)');
  $db->execute ('insert into foo (id, v1, v2) values (23, NULL, "xyxa")');
  
  my $result = $db->select ('foo', {id => {-in => [12, 23]}});
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 2;
  is $result->table_name, 'foo';
  my $invoked = 0;
  my $list = $result->all;
  isa_list_n_ok $list, 2;
  eq_or_diff $list->sort (sub { $_[0]->{id} <=> $_[1]->{id} })->to_a,
      [{id => 12, v1 => 'abc', v2 => '322'},
       {id => 23, v1 => undef, v2 => 'xyxa'}];
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
  dies_here_ok { $result->each (sub { $invoked++ }) };
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 0;
} # _select_multiple_rows_all

sub _select_multiple_rows_all_as_rows : Test(19) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');
  $db->execute ('insert into foo (id, v1, v2) values (12, "abc", 322)');
  $db->execute ('insert into foo (id, v1, v2) values (23, NULL, "xyxa")');
  
  my $result = $db->select ('foo', {id => {-in => [12, 23]}});
  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 2;
  is $result->table_name, 'foo';
  my $invoked = 0;
  my $list = $result->all_as_rows;
  isa_list_n_ok $list, 2;
  my $values = $list->sort
      (sub { $_[0]->{data}->{id} <=> $_[1]->{data}->{id} })->to_a;
  isa_ok $values->[0], 'Dongry::Table::Row';
  is $values->[0]->{db}, $db;
  is $values->[0]->{table_name}, 'foo';
  eq_or_diff $values->[0]->{data}, {id => 12, v1 => 'abc', v2 => '322'};
  isa_ok $values->[1], 'Dongry::Table::Row';
  is $values->[1]->{db}, $db;
  is $values->[1]->{table_name}, 'foo';
  eq_or_diff $values->[1]->{data}, {id => 23, v1 => undef, v2 => 'xyxa'};
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  dies_here_ok { $result->each (sub { $invoked++ }) };
  is $invoked, 0;
} # _select_multiple_rows_all_as_rows

sub _select_multiple_rows_no_return : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  {
    my $db = Dongry::Database->new
        (sources => {master => {dsn => $dsn, writable => 1},
                     default => {dsn => $dsn}});
    $db->execute ('create table foo (id int, v1 text, v2 text)');
    $db->execute ('insert into foo (id, v1, v2) values (12, "abc", 322)');
    $db->execute ('insert into foo (id, v1, v2) values (23, NULL, "xyxa")');
    
    lives_ok {
      $db->select ('foo', {id => {-in => [12, 23]}});
      undef;
    };
    undef $db;
  }
  ok 1;
} # _select_multiple_rows_no_return

sub _select_no_source : Test(2) {
  my $db = Dongry::Database->new;
  dies_here_ok {
    my $result = $db->select ('foo', {id => {-in => [12, 23]}});
  };
} # _select_no_source

sub _select_sources : Test(5) {
  reset_db_set;
  my $dsn1 = test_dsn 'dsn1';
  my $dsn2 = test_dsn 'dsn2';
  my $dsn3 = test_dsn 'dsn3';
  my $db1 = Dongry::Database->new
      (sources => {master => {dsn => $dsn1, writable => 1}});
  my $db2 = Dongry::Database->new
      (sources => {master => {dsn => $dsn2, writable => 1}});
  my $db3 = Dongry::Database->new
      (sources => {master => {dsn => $dsn3, writable => 1}});
  $db1->execute ('create table foo (id int)');
  $db2->execute ('create table foo (id int)');
  $db3->execute ('create table foo (id int)');
  $db1->execute ('insert into foo (id) values (1)');
  $db2->execute ('insert into foo (id) values (2)');
  $db3->execute ('insert into foo (id) values (3)');
  
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn1},
                   master => {dsn => $dsn2},
                   heavy => {dsn => $dsn3}});

  my $result1 = $db->select ('foo', {id => {'>=', 0}});
  is $result1->first->{id}, 1;

  my $result2 = $db->select ('foo', {id => {'>=', 0}},
                             source_name => 'default');
  is $result2->first->{id}, 1;

  my $result3 = $db->select ('foo', {id => {'>=', 0}},
                             source_name => 'master');
  is $result3->first->{id}, 2;

  my $result4 = $db->select ('foo', {id => {'>=', 0}},
                             source_name => 'heavy');
  is $result4->first->{id}, 3;
  
  dies_here_ok {
    my $result5 = $db->select ('foo', {id => {'>=', 0}},
                               source_name => 'notfound');
  };
} # _select_sources

sub _select_must_be_writable_yes : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 1},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');
  $db->execute ('insert into foo (id, v1, v2) values (12, "abc", 322)');
  $db->execute ('insert into foo (id, v1, v2) values (23, NULL, "xyxa")');
  
  my $result = $db->select ('foo', {id => {-in => [12, 23]}},
                            source_name => 'default',
                            must_be_writable => 1);
  is $result->row_count, 2;
} # _select_must_be_writable_yes

sub _select_must_be_writable_yes_2 : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');
  $db->execute ('insert into foo (id, v1, v2) values (12, "abc", 322)');
  $db->execute ('insert into foo (id, v1, v2) values (23, NULL, "xyxa")');
  
  my $result = $db->select ('foo', {id => {-in => [12, 23]}},
                            must_be_writable => 1);
  is $result->row_count, 2;
} # _select_must_be_writable_yes_2

sub _select_must_be_writable_yes_3 : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');
  $db->execute ('insert into foo (id, v1, v2) values (12, "abc", 322)');
  $db->execute ('insert into foo (id, v1, v2) values (23, NULL, "xyxa")');
  
  my $result = $db->select ('foo', {id => {-in => [12, 23]}},
                            source => 'master',
                            must_be_writable => 1);
  is $result->row_count, 2;
} # _select_must_be_writable_yes_3

sub _select_must_be_writable_no : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');
  $db->execute ('insert into foo (id, v1, v2) values (12, "abc", 322)');
  $db->execute ('insert into foo (id, v1, v2) values (23, NULL, "xyxa")');
  
  dies_here_ok {
    my $result = $db->select ('foo', {id => {-in => [12, 23]}},
                              source_name => 'default',
                              must_be_writable => 1);
  };
} # _select_must_be_writable_no

sub _select_must_be_writable_no_2 : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');
  $db->execute ('insert into foo (id, v1, v2) values (12, "abc", 322)');
  $db->execute ('insert into foo (id, v1, v2) values (23, NULL, "xyxa")');
  
  dies_here_ok {
    my $result = $db->select ('foo', {id => {-in => [12, 23]}},
                              source_name => 'default',
                              must_be_writable => 1);
  };
} # _select_must_be_writable_no_2

sub _select_distinct : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');
  $db->execute ('insert into foo (id, v1, v2) values (12, "abc", 322)');
  $db->execute ('insert into foo (id, v1, v2) values (12, "abc", 322)');
  
  my $result0 = $db->select ('foo', {id => {'>', 0}}, distinct => 0);
  eq_or_diff $result0->all->to_a,
      [{id => 12, v1 => 'abc', v2 => '322'},
       {id => 12, v1 => 'abc', v2 => '322'}];

  my $result = $db->select ('foo', {id => {'>', 0}}, distinct => 1);
  eq_or_diff $result->all->to_a, [{id => 12, v1 => 'abc', v2 => '322'}];
} # _select_distinct

sub _fields_valid : Test(29) {
  my $db = Dongry::Database->new;
  for (
    [undef, '*'],
    ['abc', '`abc`'],
    ['ab `\\c', '`ab ``\\c`'],
    ['abc', '`abc`'],
    ["\x{4010}\x{124}ab", qq{`\x{4010}\x{124}ab`}],
    [['a', 'bcd'], '`a`, `bcd`'],
    [['a', "\x{4010}\x{124}ab"], qq{`a`, `\x{4010}\x{124}ab`}],
    [['a', ['b', ['c']]] => '`a`, `b`, `c`'],
    [$db->bare_sql_fragment ('ab cde') => 'ab cde'],
    [$db->bare_sql_fragment ("ab\x{8000} cde") => "ab\x{8000} cde"],
    [['a', $db->bare_sql_fragment ('count(b) as c')] => '`a`, count(b) as c'],
    [{-count => undef} => 'COUNT(*)'],
    [{-count => 1} => 'COUNT(`1`)'],
    [{-count => 'a'} => 'COUNT(`a`)'],
    [{-count => "\x{5000}"} => qq{COUNT(`\x{5000}`)}],
    [{-count => ['a', 'b']} => 'COUNT(`a`, `b`)'],
    [{-count => 'a', as => 'ab c'} => 'COUNT(`a`) AS `ab c`'],
    [{-count => 'a', as => "\x{8000}"} => qq{COUNT(`a`) AS `\x{8000}`}],
    [{-count => undef, distinct => 1} => 'COUNT(DISTINCT *)'],
    [{-count => 'a', distinct => 1} => 'COUNT(DISTINCT `a`)'],
    [{-count => "\x{5000}", distinct => 1} => qq{COUNT(DISTINCT `\x{5000}`)}],
    [{-count => ['a', 'b'], distinct => 1} => 'COUNT(DISTINCT `a`, `b`)'],
    [{-count => 'a', distinct => 1, as => 'ab c'}
         => 'COUNT(DISTINCT `a`) AS `ab c`'],
    [{-count => 'a', distinct => 1, as => "\x{8000}"}
         => qq{COUNT(DISTINCT `a`) AS `\x{8000}`}],
    [{-min => 'a', distinct => 1, as => "\x{8000}"}
         => qq{MIN(DISTINCT `a`) AS `\x{8000}`}],
    [{-max => 'a', distinct => 1, as => "\x{8000}"}
         => qq{MAX(DISTINCT `a`) AS `\x{8000}`}],
    [{-sum => 'a', distinct => 1, as => "\x{8000}"}
         => qq{SUM(DISTINCT `a`) AS `\x{8000}`}],
    ['' => '``'],
    [['a', undef] => '`a`, *'],
  ) {
    eq_or_diff Dongry::Database::_fields $_->[0], $_->[1];
  }
} # _fields_valid

sub _fields_invalid : Test(5) {
  for (
    [],
    {},
    {-hoge => 1},
    {count => 1},
    [bless {}, 'hoge'],
  ) {
    dies_here_ok {
      Dongry::Database::_fields $_;
    };
  }
} # _fields_invalid

sub _select_fields_none : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');
  $db->execute ('insert into foo (id, v1, v2) values (12, "abc", 322)');
  $db->execute ('insert into foo (id, v1, v2) values (12, "abc", 322)');
  
  my $result = $db->select ('foo', {id => {'>', 0}}, fields => undef);
  eq_or_diff $result->all->to_a,
      [{id => 12, v1 => 'abc', v2 => '322'},
       {id => 12, v1 => 'abc', v2 => '322'}];
} # _select_fields_none

sub _select_fields_some : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');
  $db->execute ('insert into foo (id, v1, v2) values (12, "abc", 322)');
  $db->execute ('insert into foo (id, v1, v2) values (12, "abc", 322)');
  
  my $result = $db->select ('foo', {id => {'>', 0}}, fields => ['id', 'v1']);
  eq_or_diff $result->all->to_a,
      [{id => 12, v1 => 'abc'},
       {id => 12, v1 => 'abc'}];
} # _select_fields_some

sub _select_fields_utf8_flagged : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute
      (encode 'utf-8', "create table foo (id int, v1 text, `\x{6000}` text)");
  $db->execute
      (encode 'utf-8', "insert into foo (id, `\x{6000}`) values (12, 'abc')");

  my $result = $db->select ('foo', {id => {'>', 0}}, fields => ["\x{6000}"]);
  eq_or_diff $result->all->to_a,
      [{(encode 'utf-8', "\x{6000}") => 'abc'}];
} # _select_fields_utf8_flagged

sub _select_fields_utf8_unflagged : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute
      (encode 'utf-8', "create table foo (id int, v1 text, `\x{6000}` text)");
  $db->execute
      (encode 'utf-8', "insert into foo (id, `\x{6000}`) values (12, 'abc')");

  my $result = $db->select ('foo', {id => {'>', 0}},
                            fields => [encode 'utf-8', "\x{6000}"]);
  eq_or_diff $result->all->to_a,
      [{(encode 'utf-8', "\x{6000}") => 'abc'}];
} # _select_fields_utf8_unflagged

sub _select_fields_function : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');
  $db->execute ('insert into foo (id, v1, v2) values (12, "abc", 322)');
  $db->execute ('insert into foo (id, v1, v2) values (12, "abc", 322)');
  
  my $result = $db->select ('foo', {id => {'>', 0}},
                            fields => ['id', {-count => undef}]);
  eq_or_diff $result->all->to_a,
      [{id => 12, 'COUNT(*)' => '2'}];
} # _select_fields_function

sub _select_fields_function_named : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');
  $db->execute ('insert into foo (id, v1, v2) values (12, "abc", 322)');
  $db->execute ('insert into foo (id, v1, v2) values (12, "abc", 322)');
  
  my $result = $db->select ('foo', {id => {'>', 0}},
                            fields => ['id', {-count => undef, as => 'v1'}]);
  eq_or_diff $result->all->to_a,
      [{id => 12, 'v1' => '2'}];
} # _select_fields_function_named

sub _select_fields_function_distinct : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');
  $db->execute ('insert into foo (id, v1, v2) values (12, "abc", 322)');
  $db->execute ('insert into foo (id, v1, v2) values (12, "abc", 322)');
  
  my $result = $db->select
      ('foo', {id => {'>', 0}},
       fields => ['id', {-count => 'id', distinct => 1, as => 'hoge'}]);
  eq_or_diff $result->all->to_a,
      [{id => 12, 'hoge' => '1'}];
} # _select_fields_function_distinct

sub _select_fields_bare : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');
  $db->execute ('insert into foo (id, v1, v2) values (12, "abc", 322)');
  $db->execute ('insert into foo (id, v1, v2) values (10, "abc", 322)');
  
  my $result = $db->select
      ('foo', {id => {'>', 0}},
       fields => $db->bare_sql_fragment
           ('min(id) as min, max(id) as max, min(id) + max(id) as mm'));
  eq_or_diff $result->all->to_a,
      [{min => 10, max => 12, mm => 22}];
} # _select_fields_bare

sub _select_fields_column_error : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');

  dies_here_ok {
    my $result = $db->select ('foo', {id => {'>', 0}},
                              fields => $db->bare_sql_fragment ('hoge'));
  };
} # _select_fields_column_error

sub _select_fields_struct_error : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');

  dies_here_ok {
    my $result = $db->select ('foo', {id => {'>', 0}},
                              fields => [$db->bare_sql_fragment (\'hoge')]);
  };
} # _select_fields_struct_error

sub _select_fields_function_error : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 text, v2 text)');

  dies_here_ok {
    my $result = $db->select ('foo', {id => {'>', 0}}, fields => {-hoge => 1});
  };
} # _select_fields_function_error

sub _select_table_stupid : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute
      (encode 'utf-8', "create table `123 ``ab-c[` (id int, v1 text)");
  $db->execute
      (encode 'utf-8', "insert into `123 ``ab-c[` (id) value (3)");

  my $result = $db->select ("123 `ab-c[", {id => {'>', 0}});
  eq_or_diff $result->all->to_a, [{id => 3, v1 => undef}];
} # _select_table_stupid

sub _select_table_utf8_flagged : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute
      (encode 'utf-8', "create table `\x{8000}` (id int, v1 text, v2 text)");
  $db->execute
      (encode 'utf-8', "insert into `\x{8000}` (id) value (3)");

  my $result = $db->select ("\x{8000}", {id => {'>', 0}});
  eq_or_diff $result->all->to_a, [{id => 3, v1 => undef, v2 => undef}];
} # _select_table_utf8_flagged

sub _select_table_utf8_unflagged : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute
      (encode 'utf-8', "create table `\x{8000}` (id int, v1 text, v2 text)");
  $db->execute
      (encode 'utf-8', "insert into `\x{8000}` (id) value (3)");

  my $result = $db->select ((encode 'utf-8', "\x{8000}"), {id => {'>', 0}});
  eq_or_diff $result->all->to_a, [{id => 3, v1 => undef, v2 => undef}];
} # _select_table_utf8_unflagged

sub _select_where_sqla_empty : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 text)');
  $db->execute ('insert into foo (id, v1) values (12, "abvc")');
  $db->execute ('insert into foo (id, v1) values (23, "xyvc")');

  dies_here_ok {
    my $result = $db->select ('foo', {});
  };
} # _select_where_sqla_empty

sub _select_where_sqla_eq : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 text)');
  $db->execute ('insert into foo (id, v1) values (12, "abvc")');
  $db->execute ('insert into foo (id, v1) values (23, "xyvc")');

  my $result = $db->select ('foo', {id => 12});
  eq_or_diff $result->all->to_a, [{id => 12, v1 => 'abvc'}];
} # _select_where_sqla_eq

sub _select_where_sqla_ne : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 text)');
  $db->execute ('insert into foo (id, v1) values (12, "abvc")');
  $db->execute ('insert into foo (id, v1) values (23, "xyvc")');

  my $result = $db->select ('foo', {id => {'!=' => 12}});
  eq_or_diff $result->all->to_a, [{id => 23, v1 => 'xyvc'}];
} # _select_where_sqla_ne

sub _select_where_sqla_value_object : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 timestamp)');
  $db->execute ('insert into foo (id, v1) values (12, "2012-01-01 11:12:01")');
  $db->execute ('insert into foo (id, v1) values (23, "2011-05-01 12:31:12")');

  my $date = DateTime->new (year => 2011, month => 6, day => 3);
  dies_here_ok {
    my $result = $db->select ('foo', {v1 => {'>' => $date}});
  };
} # _select_where_sqla_value_object

sub _select_where_sqla_value_utf8_flagged : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 blob)');
  $db->execute (encode 'utf-8',
                qq{insert into foo (id, v1) values (12, "\x{6000}")});
  $db->execute (encode 'utf-8',
                qq{insert into foo (id, v1) values (23, "\x{6001}")});

  my $result = $db->select ('foo', {v1 => "\x{6000}"});
  eq_or_diff $result->all->to_a,
      [{id => 12, v1 => encode 'utf-8', "\x{6000}"}];
} # _select_where_sqla_utf8_flagged

sub _select_where_sqla_value_utf8_flagged_table : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute (encode 'utf-8',
                qq{create table `\x{8000}` (id int, v1 blob)});
  $db->execute (encode 'utf-8',
                qq{insert into `\x{8000}` (id, v1) values (12, "\x{6000}")});
  $db->execute (encode 'utf-8',
                qq{insert into `\x{8000}` (id, v1) values (23, "\x{6001}")});

  my $result = $db->select ("\x{8000}", {v1 => "\x{6000}"});
  eq_or_diff $result->all->to_a,
      [{id => 12, v1 => encode 'utf-8', "\x{6000}"}];
} # _select_where_sqla_utf8_flagged_table

sub _select_where_sqla_value_utf8_unflagged : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 blob)');
  $db->execute (encode 'utf-8',
                qq{insert into foo (id, v1) values (12, "\x{6000}")});
  $db->execute (encode 'utf-8',
                qq{insert into foo (id, v1) values (23, "\x{6001}")});

  my $result = $db->select ('foo', {v1 => encode 'utf-8', "\x{6000}"});
  eq_or_diff $result->all->to_a,
      [{id => 12, v1 => encode 'utf-8', "\x{6000}"}];
} # _select_where_sqla_utf8_unflagged

sub _select_where_sqla_value_utf8_unflagged_table : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute (encode 'utf-8',
                qq{create table `\x{8000}` (id int, v1 blob)});
  $db->execute (encode 'utf-8',
                qq{insert into `\x{8000}` (id, v1) values (12, "\x{6000}")});
  $db->execute (encode 'utf-8',
                qq{insert into `\x{8000}` (id, v1) values (23, "\x{6001}")});

  my $result = $db->select ("\x{8000}", {v1 => encode 'utf-8', "\x{6000}"});
  eq_or_diff $result->all->to_a,
      [{id => 12, v1 => encode 'utf-8', "\x{6000}"}];
} # _select_where_sqla_utf8_unflagged_table

sub _select_where_sqla_value_utf8_flagged_table_column : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute (encode 'utf-8',
                qq{create table `\x{8000}` (id int, `\x{9000}` blob)});
  $db->execute (encode 'utf-8',
                qq{insert into `\x{8000}` (id, `\x{9000}`)
                   values (12, "\x{6000}")});
  $db->execute (encode 'utf-8',
                qq{insert into `\x{8000}` (id, `\x{9000}`)
                   values (23, "\x{6001}")});

  my $result = $db->select ("\x{8000}", {"\x{9000}" => "\x{6000}"});
  eq_or_diff $result->all->to_a,
      [{id => 12, (encode 'utf-8', "\x{9000}") => encode 'utf-8', "\x{6000}"}];
} # _select_where_sqla_utf8_flagged_table_column

sub _select_where_sqla_value_utf8_unflagged_table_column : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute (encode 'utf-8',
                qq{create table `\x{8000}` (id int, `\x{9000}` blob)});
  $db->execute (encode 'utf-8',
                qq{insert into `\x{8000}` (id, `\x{9000}`)
                   values (12, "\x{6000}")});
  $db->execute (encode 'utf-8',
                qq{insert into `\x{8000}` (id, `\x{9000}`)
                   values (23, "\x{6001}")});

  my $result = $db->select
      ("\x{8000}", {"\x{9000}" => encode 'utf-8', "\x{6000}"});
  eq_or_diff $result->all->to_a,
      [{id => 12, (encode 'utf-8', "\x{9000}") => encode 'utf-8', "\x{6000}"}];
} # _select_where_sqla_utf8_unflagged_table_column

sub _select_where_sqla_in : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 blob)');
  $db->execute ('insert into foo (id, v1) values (12, "abc")');
  $db->execute ('insert into foo (id, v1) values (23, "de f")');

  my $result = $db->select
      ('foo', {v1 => {-in => ['abc', 'de f']}}, order => [id => 'ASC']);
  eq_or_diff $result->all->to_a,
      [{id => 12, v1 => 'abc'}, {id => 23, v1 => 'de f'}];
} # _select_where_sqla_in

sub _select_where_sqla_in_empty : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 blob)');
  $db->execute ('insert into foo (id, v1) values (12, "abc")');
  $db->execute ('insert into foo (id, v1) values (23, "de f")');

  dies_here_ok {
    my $result = $db->select ('foo', {v1 => {-in => []}});
  };
} # _select_where_sqla_in_empty

sub _select_where_sqla_in_utf8_flagged : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 blob)');
  $db->execute (encode 'utf-8',
                qq{insert into foo (id, v1) values (12, "\x{4000}")});
  $db->execute (encode 'utf-8',
                qq{insert into foo (id, v1) values (23, "\x{5001}")});

  my $result = $db->select
      ('foo', {v1 => {-in => ["\x{4000}", "\x{5001}"]}},
       order => [id => 'ASC']);
  eq_or_diff $result->all->to_a,
      [{id => 12, v1 => encode 'utf-8', "\x{4000}"},
       {id => 23, v1 => encode 'utf-8', "\x{5001}"}];
} # _select_where_sqla_in_utf8_flagged

sub _select_where_sqla_in_utf8_unflagged : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 blob)');
  $db->execute (encode 'utf-8',
                qq{insert into foo (id, v1) values (12, "\x{4000}")});
  $db->execute (encode 'utf-8',
                qq{insert into foo (id, v1) values (23, "\x{5001}")});

  my $result = $db->select
      ('foo', {v1 => {-in => [map { encode 'utf-8', $_ }
                              "\x{4000}", "\x{5001}"]}},
       order => [id => 'ASC']);
  eq_or_diff $result->all->to_a,
      [{id => 12, v1 => encode 'utf-8', "\x{4000}"},
       {id => 23, v1 => encode 'utf-8', "\x{5001}"}];
} # _select_where_sqla_in_utf8_unflagged

sub _select_where_sqla_stupid_column : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, `v1 ``2` blob)');
  $db->execute (qq{insert into foo (id, `v1 ``2`) values (12, "xyz")});
  $db->execute (qq{insert into foo (id, `v1 ``2`) values (23, "abc")});

  my $result = $db->select
      ('foo', {'v1 `2' => {-not => undef}},
       order => [id => 'ASC']);
  eq_or_diff $result->all->to_a,
      [{id => 12, 'v1 `2' => "xyz"},
       {id => 23, 'v1 `2' => "abc"}];
} # _select_where_sqla_stupid_column

sub _select_where_sqla_bad : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 blob)');
  $db->execute ('insert into foo (id, v1) values (12, "abc")');
  $db->execute ('insert into foo (id, v1) values (23, "de f")');

  dies_here_ok {
    my $result = $db->select ('foo', {v1 => {-hoge => []}});
  };
} # _select_where_sqla_bad

sub _select_where_sqla_latin1_string : Test(3) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 blob)');
  $db->execute
      (encode 'utf-8',
       qq{insert into foo (id, v1) values (12, "\x{91}\x{c1}\x{fe}")});
  $db->execute
      (encode 'latin1',
       qq{insert into foo (id, v1) values (23, "\x{91}\x{c1}\x{fe}")});

  my $result = $db->select ('foo', {v1 => "\x{91}\x{c1}\x{fe}"});
  is $result->first->{id}, 23;
  
  my $result2 = $db->select
      ('foo', {v1 => encode 'utf-8', "\x{91}\x{c1}\x{fe}"});
  is $result2->first->{id}, 12;
  
  my $result3 = $db->select
      ('foo', {v1 => encode 'latin1', "\x{91}\x{c1}\x{fe}"});
  is $result3->first->{id}, 23;
} # _select_where_sqla_latin1_string

sub _select_where_sqlp_no_args : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 blob)');
  $db->execute (qq{insert into foo (id, v1) values (12, "abc")});
  $db->execute (qq{insert into foo (id, v1) values (23, "def")});

  my $result = $db->select
      ('foo', ['id = 12 or id = 23'], order => [id => 'ASC']);
  eq_or_diff $result->all->map (sub { $_->{id} })->to_a, [12, 23];
} # _select_where_sqlp_no_args

sub _select_where_sqlp_no_args_utf8_flagged : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 blob)');
  $db->execute (qq{insert into foo (id, v1) values (12, "abc")});
  $db->execute (qq{insert into foo (id, v1) values (23, "\x{7000}")});

  my $result = $db->select ('foo', ["v1 = '\x{7000}'"]);
  eq_or_diff $result->all->map (sub { $_->{id} })->to_a, [23];
} # _select_where_sqlp_no_args_utf8_flagged

sub _select_where_sqlp_no_args_utf8_unflagged : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 blob)');
  $db->execute (qq{insert into foo (id, v1) values (12, "abc")});
  $db->execute (qq{insert into foo (id, v1) values (23, "\x{7000}")});

  my $result = $db->select ('foo', [encode 'utf-8', "v1 = '\x{7000}'"]);
  eq_or_diff $result->all->map (sub { $_->{id} })->to_a, [23];
} # _select_where_sqlp_no_args_utf8_unflagged

sub _select_where_sqlp_with_args : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 blob)');
  $db->execute (qq{insert into foo (id, v1) values (12, "abc")});
  $db->execute (qq{insert into foo (id, v1) values (23, "def")});

  my $result = $db->select
      ('foo', ['id = :id1 or id = :id2',
               id1 => 12, id2 => 23], order => [id => 'ASC']);
  eq_or_diff $result->all->map (sub { $_->{id} })->to_a, [12, 23];
} # _select_where_sqlp_with_args

sub _select_where_sqlp_with_args_2 : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 blob)');
  $db->execute (qq{insert into foo (id, v1) values (12, "abc")});
  $db->execute (qq{insert into foo (id, v1) values (23, "def")});

  my $result = $db->select
      ('foo', ['id = ? or v1 = ?',
               id => 12, v1 => 'def'], order => [id => 'ASC']);
  eq_or_diff $result->all->map (sub { $_->{id} })->to_a, [12, 23];
} # _select_where_sqlp_with_args_2

sub _select_where_sqlp_with_args_utf8_flagged : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 blob)');
  $db->execute (qq{insert into foo (id, v1) values (12, "abc")});
  $db->execute (qq{insert into foo (id, v1) values (23, "\x{7000}")});

  my $result = $db->select
      ('foo', ['v1 = ?', v1 => "\x{7000}"], order => [id => 'ASC']);
  eq_or_diff $result->all->map (sub { $_->{id} })->to_a, [23];
} # _select_where_sqlp_with_args_utf8_flagged

sub _select_where_sqlp_with_args_utf8_unflagged : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 blob)');
  $db->execute (qq{insert into foo (id, v1) values (12, "abc")});
  $db->execute (qq{insert into foo (id, v1) values (23, "\x{7000}")});

  my $result = $db->select
      ('foo', ['v1 = ?', v1 => encode 'utf-8', "\x{7000}"]);
  eq_or_diff $result->all->map (sub { $_->{id} })->to_a, [23];
} # _select_where_sqlp_with_args_utf8_unflagged

sub _select_where_bad_args : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 blob)');

  dies_here_ok {
    my $result = $db->select ('foo', 'id = 12');
  };
} # _select_where_bad_args

sub _select_where_no_args : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 blob)');

  dies_here_ok {
    my $result = $db->select ('foo', undef);
  };
} # _select_where_no_args

sub _select_where_group_by_none : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 blob)');
  $db->execute ('insert into foo (id, v1) values (12, "abc")');
  $db->execute ('insert into foo (id, v1) values (23, "def")');

  my $result = $db->select ('foo', {id => {-not => undef}}, group => undef);
  is $result->row_count, 2;
} # _select_where_group_by_none

sub _select_where_group_by_column : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 blob)');
  $db->execute ('insert into foo (id, v1) values (12, "abc")');
  $db->execute ('insert into foo (id, v1) values (12, "def")');
  $db->execute ('insert into foo (id, v1) values (23, "def")');

  my $result = $db->select ('foo', {id => {-not => undef}}, group => ['id']);
  eq_or_diff $result->all->map (sub { $_->{id} })
      ->sort (sub { $_[0] <=> $_[1] })->to_a, [12, 23];
} # _select_where_group_by_column

sub _select_where_group_by_multiple_columns : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 blob)');
  $db->execute ('insert into foo (id, v1) values (12, "abc")');
  $db->execute ('insert into foo (id, v1) values (12, "def")');
  $db->execute ('insert into foo (id, v1) values (23, "def")');

  my $result = $db->select ('foo', {id => {-not => undef}},
                            group => ['id', 'v1']);
  eq_or_diff $result->all->map (sub { $_->{id} })
      ->sort (sub { $_[0] <=> $_[1] })->to_a, [12, 12, 23];
} # _select_where_group_by_multiple_columns

sub _select_where_group_by_stupid_column : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, `v1 ``)` blob)');
  $db->execute ('insert into foo (id, `v1 ``)`) values (12, "abc")');
  $db->execute ('insert into foo (id, `v1 ``)`) values (12, "def")');
  $db->execute ('insert into foo (id, `v1 ``)`) values (23, "def")');

  my $result = $db->select ('foo', {id => {-not => undef}},
                            group => ['v1 `)']);
  eq_or_diff $result->all->map (sub { $_->{'v1 `)'} })
      ->sort (sub { $_[0] cmp $_[1] })->to_a, ['abc', 'def'];
} # _select_where_group_by_stupid_column

sub _select_where_group_by_utf8_flagged_column : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ("create table foo (id int, `\x{8000}` blob)");
  $db->execute ("insert into foo (id, `\x{8000}`) values (12, 'abc')");
  $db->execute ("insert into foo (id, `\x{8000}`) values (12, 'def')");
  $db->execute ("insert into foo (id, `\x{8000}`) values (23, 'def')");

  my $result = $db->select ('foo', {id => {-not => undef}},
                            group => ["\x{8000}"]);
  eq_or_diff $result->all->map (sub { $_->{encode 'utf-8', "\x{8000}"} })
      ->sort (sub { $_[0] cmp $_[1] })->to_a, ['abc', 'def'];
} # _select_where_group_by_utf8_flagged_column

sub _select_where_group_by_utf8_unflagged_column : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ("create table foo (id int, `\x{8000}` blob)");
  $db->execute ("insert into foo (id, `\x{8000}`) values (12, 'abc')");
  $db->execute ("insert into foo (id, `\x{8000}`) values (12, 'def')");
  $db->execute ("insert into foo (id, `\x{8000}`) values (23, 'def')");

  my $result = $db->select ('foo', {id => {-not => undef}},
                            group => [encode 'utf-8', "\x{8000}"]);
  eq_or_diff $result->all->map (sub { $_->{encode 'utf-8', "\x{8000}"} })
      ->sort (sub { $_[0] cmp $_[1] })->to_a, ['abc', 'def'];
} # _select_where_group_by_utf8_unflagged_column

sub _select_where_group_by_bad_column : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 blob)');
  $db->execute ('insert into foo (id, v1) values (12, "abc")');
  $db->execute ('insert into foo (id, v1) values (12, "def")');
  $db->execute ('insert into foo (id, v1) values (23, "def")');

  dies_here_ok {
    my $result = $db->select ('foo', {id => {-not => undef}}, group => ['v2']);
  };
} # _select_where_group_by_bad_column

sub _select_where_group_by_bad_arg : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 blob)');
  $db->execute ('insert into foo (id, v1) values (12, "abc")');
  $db->execute ('insert into foo (id, v1) values (12, "def")');
  $db->execute ('insert into foo (id, v1) values (23, "def")');

  dies_ok {
    my $result = $db->select ('foo', {id => {-not => undef}}, group => 'id');
  };
} # _select_where_group_by_bad_arg

sub _select_where_order_by_none : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 blob)');
  $db->execute ('insert into foo (id, v1) values (12, "abc")');
  $db->execute ('insert into foo (id, v1) values (12, "def")');
  $db->execute ('insert into foo (id, v1) values (23, "def")');

  my $result = $db->select ('foo', {id => {-not => undef}}, order => undef);
  is $result->row_count, 3;
} # _select_where_order_by_none

sub _select_where_order_by_a_column_implicit_order : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 blob)');
  $db->execute ('insert into foo (id, v1) values (12, "abc")');
  $db->execute ('insert into foo (id, v1) values (12, "def")');
  $db->execute ('insert into foo (id, v1) values (23, "def")');

  my $result = $db->select ('foo', {id => {-not => undef}}, order => ['id']);
  eq_or_diff $result->all->map (sub { $_->{id} })->to_a, [12, 12, 23];
} # _select_where_order_by_a_column_implicit_order

sub _select_where_order_by_a_column_asc_1 : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 blob)');
  $db->execute ('insert into foo (id, v1) values (12, "abc")');
  $db->execute ('insert into foo (id, v1) values (12, "def")');
  $db->execute ('insert into foo (id, v1) values (23, "def")');

  my $result = $db->select ('foo', {id => {-not => undef}},
                            order => ['id' => 'ASC']);
  eq_or_diff $result->all->map (sub { $_->{id} })->to_a, [12, 12, 23];
} # _select_where_order_by_a_column_asc_1

sub _select_where_order_by_a_column_asc_2 : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 blob)');
  $db->execute ('insert into foo (id, v1) values (12, "abc")');
  $db->execute ('insert into foo (id, v1) values (12, "def")');
  $db->execute ('insert into foo (id, v1) values (23, "def")');

  my $result = $db->select ('foo', {id => {-not => undef}},
                            order => ['id' => 'asc']);
  eq_or_diff $result->all->map (sub { $_->{id} })->to_a, [12, 12, 23];
} # _select_where_order_by_a_column_asc_2

sub _select_where_order_by_a_column_asc_3 : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 blob)');
  $db->execute ('insert into foo (id, v1) values (12, "abc")');
  $db->execute ('insert into foo (id, v1) values (12, "def")');
  $db->execute ('insert into foo (id, v1) values (23, "def")');

  my $result = $db->select ('foo', {id => {-not => undef}},
                            order => ['id' => 1]);
  eq_or_diff $result->all->map (sub { $_->{id} })->to_a, [12, 12, 23];
} # _select_where_order_by_a_column_asc_3

sub _select_where_order_by_a_column_asc_4 : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 blob)');
  $db->execute ('insert into foo (id, v1) values (12, "abc")');
  $db->execute ('insert into foo (id, v1) values (12, "def")');
  $db->execute ('insert into foo (id, v1) values (23, "def")');

  my $result = $db->select ('foo', {id => {-not => undef}},
                            order => ['id' => '+1']);
  eq_or_diff $result->all->map (sub { $_->{id} })->to_a, [12, 12, 23];
} # _select_where_order_by_a_column_asc_4

sub _select_where_order_by_a_column_desc_1 : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 blob)');
  $db->execute ('insert into foo (id, v1) values (12, "abc")');
  $db->execute ('insert into foo (id, v1) values (12, "def")');
  $db->execute ('insert into foo (id, v1) values (23, "def")');

  my $result = $db->select ('foo', {id => {-not => undef}},
                            order => ['id' => 'DESC']);
  eq_or_diff $result->all->map (sub { $_->{id} })->to_a, [23, 12, 12];
} # _select_where_order_by_a_column_desc_1

sub _select_where_order_by_a_column_desc_2 : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 blob)');
  $db->execute ('insert into foo (id, v1) values (12, "abc")');
  $db->execute ('insert into foo (id, v1) values (12, "def")');
  $db->execute ('insert into foo (id, v1) values (23, "def")');

  my $result = $db->select ('foo', {id => {-not => undef}},
                            order => ['id' => 'desc']);
  eq_or_diff $result->all->map (sub { $_->{id} })->to_a, [23, 12, 12];
} # _select_where_order_by_a_column_desc_2

sub _select_where_order_by_a_column_desc_3 : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 blob)');
  $db->execute ('insert into foo (id, v1) values (12, "abc")');
  $db->execute ('insert into foo (id, v1) values (12, "def")');
  $db->execute ('insert into foo (id, v1) values (23, "def")');

  my $result = $db->select ('foo', {id => {-not => undef}},
                            order => ['id' => -1]);
  eq_or_diff $result->all->map (sub { $_->{id} })->to_a, [23, 12, 12];
} # _select_where_order_by_a_column_desc_3

sub _select_where_order_by_a_column_desc_bad_order : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 blob)');
  $db->execute ('insert into foo (id, v1) values (12, "abc")');
  $db->execute ('insert into foo (id, v1) values (12, "def")');
  $db->execute ('insert into foo (id, v1) values (23, "def")');

  dies_here_ok {
    my $result = $db->select ('foo', {id => {-not => undef}},
                              order => ['id' => -10]);
  };
} # _select_where_order_by_a_column_bad_order

sub _select_where_order_by_a_column_desc_bad_order_2 : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 blob)');
  $db->execute ('insert into foo (id, v1) values (12, "abc")');
  $db->execute ('insert into foo (id, v1) values (12, "def")');
  $db->execute ('insert into foo (id, v1) values (23, "def")');

  dies_here_ok {
    my $result = $db->select ('foo', {id => {-not => undef}},
                              order => ['id' => 'random']);
  };
} # _select_where_order_by_a_column_bad_order_2

sub _select_where_order_by_multiple_columns_1 : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 blob)');
  $db->execute ('insert into foo (id, v1) values (12, "abc")');
  $db->execute ('insert into foo (id, v1) values (12, "def")');
  $db->execute ('insert into foo (id, v1) values (23, "def")');

  my $result = $db->select ('foo', {id => {-not => undef}},
                            order => [id => 1, v1 => -1]);
  eq_or_diff $result->all->to_a,
      [{id => 12, v1 => 'def'},
       {id => 12, v1 => 'abc'},
       {id => 23, v1 => 'def'}];
} # _select_where_order_by_multiple_columns_1

sub _select_where_order_by_multiple_columns_2 : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 blob)');
  $db->execute ('insert into foo (id, v1) values (12, "abc")');
  $db->execute ('insert into foo (id, v1) values (12, "def")');
  $db->execute ('insert into foo (id, v1) values (23, "def")');

  my $result = $db->select ('foo', {id => {-not => undef}},
                            order => [v1 => undef, id => -1]);
  eq_or_diff $result->all->to_a,
      [{id => 12, v1 => 'abc'},
       {id => 23, v1 => 'def'},
       {id => 12, v1 => 'def'}];
} # _select_where_order_by_multiple_columns_2

sub _select_where_order_by_stupid_column : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, `12 ``&` blob)');
  $db->execute ('insert into foo (id, `12 ``&`) values (12, "abc")');
  $db->execute ('insert into foo (id, `12 ``&`) values (12, "def")');
  $db->execute ('insert into foo (id, `12 ``&`) values (23, "def")');

  my $result = $db->select ('foo', {id => {-not => undef}},
                            order => ['12 `&' => undef, id => -1]);
  eq_or_diff $result->all->to_a,
      [{id => 12, '12 `&' => 'abc'},
       {id => 23, '12 `&' => 'def'},
       {id => 12, '12 `&' => 'def'}];
} # _select_where_order_by_stupid_column

sub _select_where_order_by_utf8_flagged_column : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ("create table foo (id int, `\x{9000}` blob)");
  $db->execute ("insert into foo (id, `\x{9000}`) values (12, 'abc')");
  $db->execute ("insert into foo (id, `\x{9000}`) values (12, 'def')");
  $db->execute ("insert into foo (id, `\x{9000}`) values (23, 'def')");

  my $result = $db->select ('foo', {id => {-not => undef}},
                            order => ["\x{9000}" => undef, id => -1]);
  eq_or_diff $result->all->to_a,
      [{id => 12, (encode 'utf-8', "\x{9000}") => 'abc'},
       {id => 23, (encode 'utf-8', "\x{9000}") => 'def'},
       {id => 12, (encode 'utf-8', "\x{9000}") => 'def'}];
} # _select_where_order_by_utf8_flagged_column

sub _select_where_order_by_utf8_unflagged_column : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ("create table foo (id int, `\x{9000}` blob)");
  $db->execute ("insert into foo (id, `\x{9000}`) values (12, 'abc')");
  $db->execute ("insert into foo (id, `\x{9000}`) values (12, 'def')");
  $db->execute ("insert into foo (id, `\x{9000}`) values (23, 'def')");

  my $result = $db->select
      ('foo', {id => {-not => undef}},
       order => [(encode 'utf-8', "\x{9000}") => undef, id => -1]);
  eq_or_diff $result->all->to_a,
      [{id => 12, (encode 'utf-8', "\x{9000}") => 'abc'},
       {id => 23, (encode 'utf-8', "\x{9000}") => 'def'},
       {id => 12, (encode 'utf-8', "\x{9000}") => 'def'}];
} # _select_where_order_by_utf8_unflagged_column

sub _select_where_order_by_bad_column : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 blob)');
  $db->execute ('insert into foo (id, v1) values (12, "abc")');
  $db->execute ('insert into foo (id, v1) values (12, "def")');
  $db->execute ('insert into foo (id, v1) values (23, "def")');

  dies_here_ok {
    my $result = $db->select ('foo', {id => {-not => undef}},
                              order => [id => 1, v2 => -1]);
  };
} # _select_where_order_by_bad_column

sub _select_where_order_by_empty_arg : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 blob)');
  $db->execute ('insert into foo (id, v1) values (12, "abc")');
  $db->execute ('insert into foo (id, v1) values (12, "def")');
  $db->execute ('insert into foo (id, v1) values (23, "def")');

  dies_here_ok {
    my $result = $db->select ('foo', {id => {-not => undef}},
                              order => []);
  };
} # _select_where_order_by_empty_arg

sub _select_where_order_by_bad_arg : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 blob)');
  $db->execute ('insert into foo (id, v1) values (12, "abc")');
  $db->execute ('insert into foo (id, v1) values (12, "def")');
  $db->execute ('insert into foo (id, v1) values (23, "def")');

  dies_ok {
    my $result = $db->select ('foo', {id => {-not => undef}},
                              order => 'id');
  };
} # _select_where_order_by_bad_arg

sub _select_where_offset_undef : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 blob)');
  $db->execute ('insert into foo (id, v1) values (12, "abc")');
  $db->execute ('insert into foo (id, v1) values (12, "def")');
  $db->execute ('insert into foo (id, v1) values (23, "def")');

  my $result = $db->select ('foo', {id => {-not => undef}},
                            order => ['id'],
                            offset => undef);
  eq_or_diff $result->all->map (sub { $_->{id} })->to_a, [12, 12, 23];
} # _select_where_offset_undef

sub _select_where_offset_zero : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 blob)');
  $db->execute ('insert into foo (id, v1) values (12, "abc")');
  $db->execute ('insert into foo (id, v1) values (12, "def")');
  $db->execute ('insert into foo (id, v1) values (23, "def")');

  my $result = $db->select ('foo', {id => {-not => undef}},
                            order => ['id'],
                            offset => 0);
  eq_or_diff $result->all->map (sub { $_->{id} })->to_a, [12];
} # _select_where_offset_zero

sub _select_where_offset_one : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 blob)');
  $db->execute ('insert into foo (id, v1) values (12, "abc")');
  $db->execute ('insert into foo (id, v1) values (12, "def")');
  $db->execute ('insert into foo (id, v1) values (23, "def")');

  my $result = $db->select ('foo', {id => {-not => undef}},
                            order => ['id'],
                            offset => 1);
  eq_or_diff $result->all->map (sub { $_->{id} })->to_a, [12];
} # _select_where_offset_one

sub _select_where_offset_large : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 blob)');
  $db->execute ('insert into foo (id, v1) values (12, "abc")');
  $db->execute ('insert into foo (id, v1) values (12, "def")');
  $db->execute ('insert into foo (id, v1) values (23, "def")');

  my $result = $db->select ('foo', {id => {-not => undef}},
                            order => ['id'],
                            offset => 1000);
  eq_or_diff $result->all->map (sub { $_->{id} })->to_a, [];
} # _select_where_offset_large

sub _select_where_offset_bad : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 blob)');
  $db->execute ('insert into foo (id, v1) values (12, "abc")');
  $db->execute ('insert into foo (id, v1) values (12, "def")');
  $db->execute ('insert into foo (id, v1) values (23, "def")');

  my $result = $db->select ('foo', {id => {-not => undef}},
                            order => ['id'],
                            offset => 'anbc');
  eq_or_diff $result->all->map (sub { $_->{id} })->to_a, [12];
} # _select_where_offset_bad

sub _select_where_limit_none : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 blob)');
  $db->execute ('insert into foo (id, v1) values (12, "abc")');
  $db->execute ('insert into foo (id, v1) values (12, "def")');
  $db->execute ('insert into foo (id, v1) values (23, "def")');

  my $result = $db->select ('foo', {id => {-not => undef}},
                            order => ['id'],
                            limit => undef);
  is $result->row_count, 3;
} # _select_where_limit_none

sub _select_where_limit_zero : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 blob)');
  $db->execute ('insert into foo (id, v1) values (12, "abc")');
  $db->execute ('insert into foo (id, v1) values (12, "def")');
  $db->execute ('insert into foo (id, v1) values (23, "def")');

  my $result = $db->select ('foo', {id => {-not => undef}},
                            order => ['id'],
                            limit => 0);
  eq_or_diff $result->all->map (sub { $_->{id} })->to_a, [12];
} # _select_where_limit_zero

sub _select_where_limit_one : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 blob)');
  $db->execute ('insert into foo (id, v1) values (12, "abc")');
  $db->execute ('insert into foo (id, v1) values (12, "def")');
  $db->execute ('insert into foo (id, v1) values (23, "def")');

  my $result = $db->select ('foo', {id => {-not => undef}},
                            order => ['id'],
                            limit => 1);
  eq_or_diff $result->all->map (sub { $_->{id} })->to_a, [12];
} # _select_where_limit_one

sub _select_where_limit_two : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 blob)');
  $db->execute ('insert into foo (id, v1) values (12, "abc")');
  $db->execute ('insert into foo (id, v1) values (12, "def")');
  $db->execute ('insert into foo (id, v1) values (23, "def")');

  my $result = $db->select ('foo', {id => {-not => undef}},
                            order => ['id'],
                            limit => 2);
  eq_or_diff $result->all->map (sub { $_->{id} })->to_a, [12, 12];
} # _select_where_limit_two

sub _select_where_limit_many : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 blob)');
  $db->execute ('insert into foo (id, v1) values (12, "abc")');
  $db->execute ('insert into foo (id, v1) values (12, "def")');
  $db->execute ('insert into foo (id, v1) values (23, "def")');

  my $result = $db->select ('foo', {id => {-not => undef}},
                            order => ['id'],
                            limit => 10000);
  eq_or_diff $result->all->map (sub { $_->{id} })->to_a, [12, 12, 23];
} # _select_where_limit_many

sub _select_where_limit_bad : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 blob)');
  $db->execute ('insert into foo (id, v1) values (12, "abc")');
  $db->execute ('insert into foo (id, v1) values (12, "def")');
  $db->execute ('insert into foo (id, v1) values (23, "def")');

  my $result = $db->select ('foo', {id => {-not => undef}},
                            order => ['id'],
                            limit => 'bc a');
  eq_or_diff $result->all->map (sub { $_->{id} })->to_a, [];
} # _select_where_limit_bad

sub _select_where_limit_bad_2 : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 blob)');
  $db->execute ('insert into foo (id, v1) values (12, "abc")');
  $db->execute ('insert into foo (id, v1) values (12, "def")');
  $db->execute ('insert into foo (id, v1) values (23, "def")');

  my $result = $db->select ('foo', {id => {-not => undef}},
                            order => ['id'],
                            limit => '2,5');
  eq_or_diff $result->all->map (sub { $_->{id} })->to_a, [12, 12];
} # _select_where_limit_bad_2

sub _select_where_offset_limit_none : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 blob)');
  $db->execute ('insert into foo (id, v1) values (12, "abc")');
  $db->execute ('insert into foo (id, v1) values (12, "def")');
  $db->execute ('insert into foo (id, v1) values (23, "def")');

  my $result = $db->select ('foo', {id => {-not => undef}},
                            order => ['id'],
                            offset => undef,
                            limit => undef);
  eq_or_diff $result->all->map (sub { $_->{id} })->to_a, [12, 12, 23];
} # _select_where_offset_limit_none

sub _select_where_offset_limit_zero_zero : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 blob)');
  $db->execute ('insert into foo (id, v1) values (12, "abc")');
  $db->execute ('insert into foo (id, v1) values (12, "def")');
  $db->execute ('insert into foo (id, v1) values (23, "def")');

  my $result = $db->select ('foo', {id => {-not => undef}},
                            order => ['id'],
                            offset => 0,
                            limit => 0);
  eq_or_diff $result->all->map (sub { $_->{id} })->to_a, [12];
} # _select_where_offset_limit_zero_zero

sub _select_where_offset_limit_one_one : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 blob)');
  $db->execute ('insert into foo (id, v1) values (12, "abc")');
  $db->execute ('insert into foo (id, v1) values (12, "def")');
  $db->execute ('insert into foo (id, v1) values (23, "def")');

  my $result = $db->select ('foo', {id => {-not => undef}},
                            order => ['id'],
                            offset => 1,
                            limit => 1);
  eq_or_diff $result->all->map (sub { $_->{id} })->to_a, [12];
} # _select_where_offset_limit_one_one

sub _select_where_offset_limit_2_1 : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 blob)');
  $db->execute ('insert into foo (id, v1) values (12, "abc")');
  $db->execute ('insert into foo (id, v1) values (12, "def")');
  $db->execute ('insert into foo (id, v1) values (23, "def")');

  my $result = $db->select ('foo', {id => {-not => undef}},
                            order => ['id'],
                            offset => 2,
                            limit => 1);
  eq_or_diff $result->all->map (sub { $_->{id} })->to_a, [23];
} # _select_where_offset_limit_2_1

sub _select_where_offset_limit_1_2 : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0},
                   master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int, v1 blob)');
  $db->execute ('insert into foo (id, v1) values (12, "abc")');
  $db->execute ('insert into foo (id, v1) values (12, "def")');
  $db->execute ('insert into foo (id, v1) values (23, "def")');

  my $result = $db->select ('foo', {id => {-not => undef}},
                            order => ['id'],
                            offset => 1,
                            limit => 2);
  eq_or_diff $result->all->map (sub { $_->{id} })->to_a, [12, 23];
} # _select_where_offset_limit_1_2

__PACKAGE__->runtests;

1;

=head1 LICENSE

Copyright 2011 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
