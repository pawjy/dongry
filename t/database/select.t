package test::Dongry::Database::select;
use strict;
use warnings;
use Path::Class;
use lib file (__FILE__)->dir->parent->subdir ('lib')->stringify;
use Test::Dongry;
use base qw(Test::Class);
use Dongry::Database;
use Encode;

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
  dies_ok { $result->each (sub { $invoked++ }) };
  dies_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 0;
  dies_ok { $result->all };
  dies_ok { $result->all_as_rows };
  dies_ok { $result->first };
  dies_ok { $result->first_as_row };
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
  dies_ok { $result->each_as_row (sub { $invoked++ }) };
  dies_ok { $result->each (sub { $invoked++ }) };
  is $invoked, 0;
  dies_ok { $result->all };
  dies_ok { $result->all_as_rows };
  dies_ok { $result->first };
  dies_ok { $result->first_as_row };
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
  dies_ok { $result->first };
  dies_ok { $result->first_as_row };
  my $invoked = 0;
  dies_ok { $result->each (sub { $invoked++ }) };
  dies_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 0;
  dies_ok { $result->all };
  dies_ok { $result->all_as_rows };
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
  dies_ok { $result->first };
  dies_ok { $result->first_as_row };
  my $invoked = 0;
  dies_ok { $result->each_as_row (sub { $invoked++ }) };
  dies_ok { $result->each (sub { $invoked++ }) };
  is $invoked, 0;
  dies_ok { $result->all };
  dies_ok { $result->all_as_rows };
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
  dies_ok { $result->all };
  dies_ok { $result->all_as_rows };
  dies_ok { $result->first };
  dies_ok { $result->first_as_row };
  my $invoked = 0;
  dies_ok { $result->each (sub { $invoked++ }) };
  dies_ok { $result->each_as_row (sub { $invoked++ }) };
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
  dies_ok { $result->all };
  dies_ok { $result->all_as_rows };
  dies_ok { $result->first };
  dies_ok { $result->first_as_row };
  my $invoked = 0;
  dies_ok { $result->each_as_row (sub { $invoked++ }) };
  dies_ok { $result->each (sub { $invoked++ }) };
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
  dies_ok { $result->each (sub { $invoked++ }) };
  dies_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 1;
  dies_ok { $result->all };
  dies_ok { $result->all_as_rows };
  dies_ok { $result->first };
  dies_ok { $result->first_as_row };
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
  dies_ok { $result->each_as_row (sub { $invoked++ }) };
  dies_ok { $result->each (sub { $invoked++ }) };
  is $invoked, 1;
  dies_ok { $result->all };
  dies_ok { $result->all_as_rows };
  dies_ok { $result->first };
  dies_ok { $result->first_as_row };
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
  dies_ok { $result->first };
  dies_ok { $result->first_as_row };
  dies_ok { $result->each (sub { $invoked++ }) };
  dies_ok { $result->each_as_row (sub { $invoked++ }) };
  is $invoked, 0;
  dies_ok { $result->all };
  dies_ok { $result->all_as_rows };
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
  dies_ok { $result->first };
  dies_ok { $result->first_as_row };
  dies_ok { $result->each_as_row (sub { $invoked++ }) };
  dies_ok { $result->each (sub { $invoked++ }) };
  is $invoked, 0;
  dies_ok { $result->all };
  dies_ok { $result->all_as_rows };
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
  dies_ok { $result->all };
  dies_ok { $result->all_as_rows };
  dies_ok { $result->first };
  dies_ok { $result->first_as_row };
  dies_ok { $result->each (sub { $invoked++ }) };
  dies_ok { $result->each_as_row (sub { $invoked++ }) };
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
  dies_ok { $result->all };
  dies_ok { $result->all_as_rows };
  dies_ok { $result->first };
  dies_ok { $result->first_as_row };
  dies_ok { $result->each_as_row (sub { $invoked++ }) };
  dies_ok { $result->each (sub { $invoked++ }) };
  is $invoked, 0;
} # _select_a_row_all_as_rows

__PACKAGE__->runtests;

1;
