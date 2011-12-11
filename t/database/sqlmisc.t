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

  dies_ok {
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

__PACKAGE__->runtests;

1;
