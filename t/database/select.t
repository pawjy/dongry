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

__PACKAGE__->runtests;

1;
