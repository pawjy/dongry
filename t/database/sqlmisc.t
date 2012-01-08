package test::Dongry::Database::sqlmisc;
use strict;
use warnings;
use Path::Class;
use lib file (__FILE__)->dir->parent->subdir ('lib')->stringify;
use Test::Dongry;
use base qw(Test::Class);
use Dongry::Database;
use Encode;
use DateTime::Format::MySQL;

sub _set_tz_default : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->set_tz;

  is $db->execute ('select now() as now', undef, source_name => 'master')
      ->first->{now},
      DateTime::Format::MySQL->format_datetime
          (DateTime->now (time_zone => 'UTC'));
} # _set_tz_default

sub _set_tz_utc : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->set_tz ('+00:00');

  is $db->execute ('select now() as now', undef, source_name => 'master')
      ->first->{now},
      DateTime::Format::MySQL->format_datetime
          (DateTime->now (time_zone => 'UTC'));
} # _set_tz_utc

sub _set_tz_jst : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->set_tz ('+09:00');

  is $db->execute ('select now() as now', undef, source_name => 'master')
      ->first->{now},
      DateTime::Format::MySQL->format_datetime
          (DateTime->now (time_zone => '+09:00'));
} # _set_tz_jst

sub _set_tz_bad : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->set_tz ('+09:00');

  dies_here_ok {
    $db->set_tz ('unknown');
  };

  is $db->execute ('select now() as now', undef, source_name => 'master')
      ->first->{now},
      DateTime::Format::MySQL->format_datetime
          (DateTime->now (time_zone => '+09:00'));
} # _set_tz_bad

sub _set_tz_source_name : Test(12) {
  reset_db_set;

  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn},
                   default => {dsn => $dsn},
                   heavy => {dsn => $dsn}});
  $db->set_tz (undef, source_name => 'master');
  $db->set_tz (undef, source_name => 'default');
  $db->set_tz (undef, source_name => 'heavy');

  $db->set_tz ('+01:00');

  is $db->execute ('select now() as now', undef, source_name => 'master')
      ->first->{now},
      DateTime::Format::MySQL->format_datetime
          (DateTime->now (time_zone => '+01:00'));
  is $db->execute ('select now() as now', undef, source_name => 'default')
      ->first->{now},
      DateTime::Format::MySQL->format_datetime
          (DateTime->now (time_zone => '+00:00'));
  is $db->execute ('select now() as now', undef, source_name => 'heavy')
      ->first->{now},
      DateTime::Format::MySQL->format_datetime
          (DateTime->now (time_zone => '+00:00'));

  $db->set_tz ('+02:00', source_name => 'master');

  is $db->execute ('select now() as now', undef, source_name => 'master')
      ->first->{now},
      DateTime::Format::MySQL->format_datetime
          (DateTime->now (time_zone => '+02:00'));
  is $db->execute ('select now() as now', undef, source_name => 'default')
      ->first->{now},
      DateTime::Format::MySQL->format_datetime
          (DateTime->now (time_zone => '+00:00'));
  is $db->execute ('select now() as now', undef, source_name => 'heavy')
      ->first->{now},
      DateTime::Format::MySQL->format_datetime
          (DateTime->now (time_zone => '+00:00'));

  $db->set_tz ('+03:00', source_name => 'default');

  is $db->execute ('select now() as now', undef, source_name => 'master')
      ->first->{now},
      DateTime::Format::MySQL->format_datetime
          (DateTime->now (time_zone => '+02:00'));
  is $db->execute ('select now() as now', undef, source_name => 'default')
      ->first->{now},
      DateTime::Format::MySQL->format_datetime
          (DateTime->now (time_zone => '+03:00'));
  is $db->execute ('select now() as now', undef, source_name => 'heavy')
      ->first->{now},
      DateTime::Format::MySQL->format_datetime
          (DateTime->now (time_zone => '+00:00'));

  $db->set_tz ('+04:00', source_name => 'heavy');

  is $db->execute ('select now() as now', undef, source_name => 'master')
      ->first->{now},
      DateTime::Format::MySQL->format_datetime
          (DateTime->now (time_zone => '+02:00'));
  is $db->execute ('select now() as now', undef, source_name => 'default')
      ->first->{now},
      DateTime::Format::MySQL->format_datetime
          (DateTime->now (time_zone => '+03:00'));
  is $db->execute ('select now() as now', undef, source_name => 'heavy')
      ->first->{now},
      DateTime::Format::MySQL->format_datetime
          (DateTime->now (time_zone => '+04:00'));
} # _set_tz_source_name

sub _set_tz_cb : Test(5) {
  my $db = new_db;
  my $invoked;
  $db->set_tz ('', cb => sub {
    is $_[0], $db;
    isa_ok $_[1], 'Dongry::Database::Executed';
    ok $_[1]->is_success;
    ng $_[1]->is_error;
    $invoked++;
  });
  is $invoked, 1;
} # _set_tz_cb

sub _set_tz_cb_bad : Test(2) {
  my $db = new_db;
  my $invoked;
  dies_here_ok {
    $db->set_tz ('hoge', cb => sub {
      $invoked++;
    });
  };
  ng $invoked;
} # _set_tz_cb_bad

sub _set_tz_cb_croak : Test(1) {
  my $db = new_db;
  eval {
    $db->set_tz ('', cb => sub {
      Carp::croak 'hoge';
    });
  };
  is $@, 'hoge at ' . __FILE__ . ' line ' . (__LINE__ - 2) . "\n";
} # _set_tz_cb_croak

__PACKAGE__->runtests;

1;

=head1 LICENSE

Copyright 2011-2012 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
