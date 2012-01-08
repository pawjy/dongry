package test::Dongry::Database::anyevent::sql;
use strict;
use warnings;
use Path::Class;
use lib file (__FILE__)->dir->parent->subdir ('lib')->stringify;
use Test::Dongry;
use base qw(Test::Class);
use Dongry::Database;
use AnyEvent;

sub _set_tz_cb : Test(7) {
  my $db = new_db;
  $db->source (ae => {dsn => $db->source ('master')->{dsn}, anyevent => 1,
                      writable => 1});

  my $cv = AnyEvent->condvar;
  $cv->begin;

  my $result;
  $cv->begin;
  $db->set_tz ('+01:00', cb => sub {
    is $_[0], $db;
    $result = $_[1];
    $cv->end;
  }, source_name => 'ae');

  my $tz;
  $cv->begin;
  $db->execute ('SELECT @@session.time_zone AS tz', undef, cb => sub {
    $tz = $_[1]->first->{tz};
    $cv->end;
  }, source_name => 'ae');

  $cv->end;
  $cv->recv;

  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  ng $result->error_text;
  ng $result->error_sql;
  is $tz, '+01:00';
} # _set_tz_cb

sub _set_tz_error : Test(7) {
  my $db = new_db;
  $db->source (ae => {dsn => $db->source ('master')->{dsn}, anyevent => 1,
                      writable => 1});

  my $cv = AnyEvent->condvar;
  $cv->begin;

  my $result;
  $cv->begin;
  $db->set_tz ('ho ge', cb => sub {
    is $_[0], $db;
    $result = $_[1];
    $cv->end;
  }, source_name => 'ae');

  my $tz;
  $cv->begin;
  $db->execute ('SELECT @@session.time_zone AS tz', undef, cb => sub {
    $tz = $_[1]->first->{tz} if $_[1]->is_success;
    $cv->end;
  }, source_name => 'ae');

  $cv->end;
  $cv->recv;

  isa_ok $result, 'Dongry::Database::Executed';
  ng $result->is_success;
  ok $result->is_error;
  like $result->error_text, qr{time zone};
  is $result->error_sql, 'SET time_zone = ?';
  isnt $tz, 'ho ge';
} # _set_tz_error

__PACKAGE__->runtests;

$Dongry::LeakTest = 1;

1;

=head1 LICENSE

Copyright 2012 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
