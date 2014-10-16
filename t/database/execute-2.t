use strict;
use warnings;
use Path::Tiny;
use lib glob path (__FILE__)->parent->parent->parent->child ('t/lib');
use lib glob path (__FILE__)->parent->parent->parent->child ('t_deps/modules/*/lib');
use Test::X1;
use Test::Dongry;
use Dongry::Database;

my $dsn = test_dsn 'hoge1';

test {
  my $c = shift;

  my $db = Dongry::Database->new
      (sources => {dbi => {dsn => $dsn, writable => 1}});

  $db->execute ('create table foo1 (id int)', undef, source_name => 'dbi');
  $db->execute ('insert into foo1 (id) values (3)', undef, source_name => 'dbi');

  my @row;
  $db->execute ('select * from foo1', undef, source_name => 'dbi', each_cb => sub {
    push @row, $_;
  });

  is scalar @row, 1;
  eq_or_diff $row[0], {id => 3};

  done $c;
} n => 2, name => 'each_cb';

test {
  my $c = shift;

  my $db = Dongry::Database->new
      (sources => {dbi => {dsn => $dsn, writable => 1}});

  $db->execute ('create table foo2 (id int)', undef, source_name => 'dbi');
  $db->execute ('insert into foo2 (id) values (3)', undef, source_name => 'dbi');

  my @row;
  $db->execute ('select * from foo2', undef, source_name => 'dbi', each_as_row_cb => sub {
    push @row, $_;
  }, table_name => 'hogefuga');

  is scalar @row, 1;
  isa_ok $row[0], 'Dongry::Table::Row';
  is $row[0]->table_name, 'hogefuga';
  is $row[0]->get ('id'), 3;

  done $c;
} n => 4, name => 'each_as_row_cb';

test {
  my $c = shift;

  my $db = Dongry::Database->new
      (sources => {dbi => {dsn => $dsn, writable => 1}});

  $db->execute ('create table foo3 (id int)', undef, source_name => 'dbi');
  $db->execute ('insert into foo3 (id) values (3)', undef, source_name => 'dbi');

  my $result = $db->execute ('select * from foo3', undef, source_name => 'dbi', each_as_row_cb => sub { }, table_name => 'hogefuga');

  dies_here_ok { $result->each (sub { }) };
  dies_here_ok { $result->each_as_row (sub { }) };
  dies_here_ok { $result->all };
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->first };
  dies_here_ok { $result->first_as_row };

  done $c;
} n => 6, name => 'each_as_row_cb result';

test {
  my $c = shift;

  my $db = Dongry::Database->new
      (sources => {dbi => {dsn => $dsn, writable => 1}});

  $db->execute ('create table foo4 (id int)', undef, source_name => 'dbi');
  $db->execute ('insert into foo4 (id) values (3)', undef, source_name => 'dbi');

  my $invoked = 0;
  dies_here_ok {
    $db->execute ('select * from foo4', undef, source_name => 'dbi', each_as_row_cb => sub { $invoked++ });
  };
  is $invoked, 0;

  done $c;
} n => 2, name => 'each_as_row_cb no table_name';

run_tests;

=head1 LICENSE

Copyright 2011-2014 Wakaba <wakaba@suikawiki.org>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
