package test::Dongry::Database::errorrecovery;
use strict;
use warnings;
use Path::Class;
use lib file (__FILE__)->dir->parent->subdir ('lib')->stringify;
use Test::Dongry;
use base qw(Test::Class);
use Dongry::Database;

sub _deadlock_retry_disabled_1 : Test(4) {
  reset_db_set;
  my $dsn = test_dsn 'test1';

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  my $db2 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});

  $db->execute ('create table foo (id int, v1 int) engine=innodb');
  $db->insert ('foo', [{id => 1, v1 => 120}]);

  my $time = time;

  my $transaction = $db->transaction;
  my $count = $db->select ('foo', {id => 1}, lock => 'update')->first->{v1};

  my $transaction2 = $db2->transaction;
  dies_here_ok {
    $db2->select ('foo', {id => 1}, lock => 'update');
  };

  $db->update ('foo', {v1 => $count + 1}, where => {id => 1});
  $transaction->commit;

  $db2->update ('foo', {v1 => $count + 4}, where => {id => 1});
  $transaction2->commit;

  my $count2 = $db2->select ('foo', {id => 1})->first->{v1};
  is $count2, $count + 4;

  ok not time - $time > 4;

  my $result = $db->execute ('select * from foo order by id asc');
  eq_or_diff $result->all->to_a, [{id => 1, v1 => 120 + 4}];
} # _deadlock_retry_disabled_1

sub _deadlock_retry_enabled_1 : Test(4) {
  reset_db_set;
  my $dsn = test_dsn 'test1';

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  my $db2 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});

  $db->execute ('create table foo (id int, v1 int) engine=innodb');
  $db->insert ('foo', [{id => 1, v1 => 120}]);

  local $Dongry::Database::RetryIfDeadlock = 1;

  my $time = time;

  my $transaction = $db->transaction;
  my $count = $db->select ('foo', {id => 1}, lock => 'update')->first->{v1};

  my $transaction2 = $db2->transaction;
  dies_here_ok {
    $db2->select ('foo', {id => 1}, lock => 'update');
  };

  $db->update ('foo', {v1 => $count + 1}, where => {id => 1});
  $transaction->commit;

  $db2->update ('foo', {v1 => $count + 4}, where => {id => 1});
  $transaction2->commit;

  my $count2 = $db2->select ('foo', {id => 1})->first->{v1};
  is $count2, $count + 4;

  ok time - $time > 4;

  my $result = $db->execute ('select * from foo order by id asc');
  eq_or_diff $result->all->to_a, [{id => 1, v1 => 120 + 4}];
} # _deadlock_retry_enabled_1

__PACKAGE__->runtests;

1;

=head1 LICENSE

Copyright 2013 Wakaba <wakaba@suikawiki.org>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
