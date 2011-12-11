package test::Dongry::Database::update;
use strict;
use warnings;
use Path::Class;
use lib file (__FILE__)->dir->parent->subdir ('lib')->stringify;
use Test::Dongry;
use base qw(Test::Class);
use Dongry::Database;
use Encode;

sub _update_nop : Test(66) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int)');
  
  for my $method (qw(all all_as_rows each each_as_rows first first_as_row)) {
    my $result = $db->update ('foo', {id => 23}, {id => 12});
    isa_ok $result, 'Dongry::Database::Executed';
    is $result->row_count, 0;
    is $result->table_name, 'foo';
    my $invoked = 0;
    dies_ok { $result->$method (sub { $invoked++ }) };
    dies_ok { $result->all };
    dies_ok { $result->all_as_rows };
    dies_ok { $result->each (sub { $invoked++ }) };
    dies_ok { $result->each_as_row (sub { $invoked++ }) };
    is $invoked, 0;
    dies_ok { $result->first };
    dies_ok { $result->first_as_row };
  }
} # _update_nop

sub _update_a_row_updated : Test(72) {
  for my $method (qw(all all_as_rows each each_as_rows first first_as_row)) {
    reset_db_set;
    my $dsn = test_dsn 'test1';
    my $db = Dongry::Database->new
        (sources => {master => {dsn => $dsn, writable => 1}});
    $db->execute ('create table foo (id int)');
    $db->execute ('insert into foo (id) values (12)');
    
    my $result = $db->update ('foo', {id => 23}, {id => 12});
    isa_ok $result, 'Dongry::Database::Executed';
    is $result->row_count, 1;
    is $result->table_name, 'foo';
    my $invoked = 0;
    dies_ok { $result->$method (sub { $invoked++ }) };
    dies_ok { $result->all };
    dies_ok { $result->all_as_rows };
    dies_ok { $result->each (sub { $invoked++ }) };
    dies_ok { $result->each_as_row (sub { $invoked++ }) };
    is $invoked, 0;
    dies_ok { $result->first };
    dies_ok { $result->first_as_row };

    eq_or_diff $db->execute
        ('select * from foo', undef, source_name => 'master')->all->to_a,
        [{id => 23}];
  }
} # _update_a_row_updated

sub _update_two_row_updated : Test(72) {
  for my $method (qw(all all_as_rows each each_as_rows first first_as_row)) {
    reset_db_set;
    my $dsn = test_dsn 'test1';
    my $db = Dongry::Database->new
        (sources => {master => {dsn => $dsn, writable => 1}});
    $db->execute ('create table foo (id int)');
    $db->execute ('insert into foo (id) values (12), (12), (13)');
    
    my $result = $db->update ('foo', {id => 23}, {id => 12});
    isa_ok $result, 'Dongry::Database::Executed';
    is $result->row_count, 2;
    is $result->table_name, 'foo';
    my $invoked = 0;
    dies_ok { $result->$method (sub { $invoked++ }) };
    dies_ok { $result->all };
    dies_ok { $result->all_as_rows };
    dies_ok { $result->each (sub { $invoked++ }) };
    dies_ok { $result->each_as_row (sub { $invoked++ }) };
    is $invoked, 0;
    dies_ok { $result->first };
    dies_ok { $result->first_as_row };

    eq_or_diff $db->select
        ('foo', ['1 = 1'],
         source_name => 'master',
         order => [id => 1])->all->to_a,
             [{id => 13}, {id => 23}, {id => 23}];
  }
} # _update_two_rows_updated

__PACKAGE__->runtests;

1;
