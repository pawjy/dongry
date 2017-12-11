package Test::Dongry;
use strict;
use warnings;
use Path::Tiny;
use lib glob path (__FILE__)->parent->parent->parent->parent->child ('lib');
use lib glob path (__FILE__)->parent->parent->parent->parent->child ('modules/*/lib');
use lib glob path (__FILE__)->parent->parent->parent->parent->child ('t_deps/modules/*/lib');
use Promised::Mysqld;

use Exporter::Lite;
our @EXPORT;

use Test::MoreMore;
use Test::X1;
push @EXPORT, @Test::MoreMore::EXPORT;
push @EXPORT, @Test::X1::EXPORT;

require DBIx::ShowSQL;

my $Mysqld = Promised::Mysqld->new;
note "Start mysqld...";
$Mysqld->start->to_cv->recv;
note "Mysqld started";

## For Test::Class tests
push @EXPORT, qw(runtests);
sub runtests {
  Test::Class->runtests (@_);
  note "Stop mysqld...";
  $Mysqld->stop->to_cv->recv;
  undef $Mysqld;
  note "Mysqld stopped";
} # runtests

## For Test::X1 tests
push @EXPORT, qw(RUN);
sub RUN () {
  run_tests;
  note "Stop mysqld...";
  $Mysqld->stop->to_cv->recv;
  undef $Mysqld;
  note "Mysqld stopped";
} # RUN

my $DBNumber = 1;

push @EXPORT, qw(reset_db_set);
sub reset_db_set () {
  $DBNumber++;
} # reset_db_set

push @EXPORT, qw(test_dsn);
sub test_dsn ($) {
  my $name = shift || die;
  $name .= '_' . $DBNumber . '_test';
  my $dsn = $Mysqld->get_dsn_string (dbname => $name);

  $Mysqld->create_db_and_execute_sqls ($name, [])->to_cv->recv;

  return $dsn;
} # test_dsn

push @EXPORT, qw(new_db);
sub new_db (%) {
  my %args = @_;
  reset_db_set ();
  my $dsn = test_dsn ('test1');
  require Dongry::Database;
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}},
       schema => $args{schema});
  for my $name (keys %{$args{schema} || {}}) {
    if ($args{schema}->{$name}->{_create}) {
      $db->execute ($args{schema}->{$name}->{_create});
    }
  }
  if ($args{ae}) {
    $db->source (ae => {dsn => $dsn, writable => 1, anyevent => 1});
  }
  return $db;
} # new_db

1;

=head1 LICENSE

Copyright 2011-2017 Wakaba <wakaba@suikawiki.org>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
