package test::Dongry::Database::anyevent::embedcaller;
use strict;
use warnings;
use Path::Class;
use lib file (__FILE__)->dir->parent->subdir ('lib')->stringify;
use Test::Dongry;
use base qw(Test::Class);
use Dongry::Database;
use AnyEvent;

sub _execute_plain : Test(1) {
  my $db = new_db;
  $db->source (ae => {dsn => $db->source ('master')->{dsn}, anyevent => 1,
                      writable => 1});

  my $cv = AnyEvent->condvar;

  my $sql;
  $db->execute ('show tables', undef,
                cb => sub { $sql = $_[0]->{last_sql}; $cv->send },
                source_name => 'ae');

  $cv->recv;

  is $sql, 'show tables';
} # _execute_plain

sub _execute_embedded : Test(1) {
  my $db = new_db;
  $db->source (ae => {dsn => $db->source ('master')->{dsn}, anyevent => 1,
                      writable => 1});

  local $Dongry::Database::EmbedCallerInSQL = 1;

  my $cv = AnyEvent->condvar;

  my $sql;
  $db->execute ('show tables', undef,
                cb => sub { $sql = $_[0]->{last_sql}; $cv->send },
                source_name => 'ae');

  $cv->recv;

  is $sql,
      'show tables /* ae at '.__FILE__.' line '.(__LINE__-5).' */';
} # _execute_embedded

sub _select_embedded : Test(1) {
  my $db = new_db;
  $db->execute ('create table foo (id int)');
  $db->source (ae => {dsn => $db->source ('master')->{dsn}, anyevent => 1,
                      writable => 1});

  local $Dongry::Database::EmbedCallerInSQL = 1;
  my $cv = AnyEvent->condvar;

  my $sql;
  $db->select ('foo', {id => 1},
               cb => sub { $sql = $_[0]->{last_sql}; $cv->send },
               source_name => 'ae');

  $cv->recv;

  is $sql,
      'SELECT * FROM `foo` WHERE `id` = ? /* ae at '.__FILE__.
      ' line '.(__LINE__-6).' */';
} # _select_embedded

sub _insert_embedded : Test(1) {
  my $db = new_db;
  $db->execute ('create table foo (id int)');
  $db->source (ae => {dsn => $db->source ('master')->{dsn}, anyevent => 1,
                      writable => 1});

  local $Dongry::Database::EmbedCallerInSQL = 1;
  my $cv = AnyEvent->condvar;

  my $sql;
  $db->insert ('foo', [{id => 1}],
               cb => sub { $sql = $_[0]->{last_sql}; $cv->send },
               source_name => 'ae');

  $cv->recv;

  is $sql,
      'INSERT INTO `foo` (`id`) VALUES (?) /* ae at '.__FILE__.
      ' line '.(__LINE__-6).' */';
} # _insert_embedded

sub _update_embedded : Test(1) {
  my $db = new_db;
  $db->execute ('create table foo (id int)');
  $db->execute ('insert into foo (id) values (1)');
  $db->source (ae => {dsn => $db->source ('master')->{dsn}, anyevent => 1,
                      writable => 1});

  local $Dongry::Database::EmbedCallerInSQL = 1;
  my $cv = AnyEvent->condvar;

  my $sql;
  $db->update ('foo', {id => 2}, where => {id => 1},
               cb => sub { $sql = $_[0]->{last_sql}; $cv->send },
               source_name => 'ae');

  $cv->recv;

  is $sql,
      'UPDATE `foo` SET `id` = ? WHERE `id` = ? /* ae at '.__FILE__.
      ' line '.(__LINE__-6).' */';
} # _update_embedded

sub _delete_embedded : Test(1) {
  my $db = new_db;
  $db->execute ('create table foo (id int)');
  $db->execute ('insert into foo (id) values (1)');
  $db->source (ae => {dsn => $db->source ('master')->{dsn}, anyevent => 1,
                      writable => 1});

  local $Dongry::Database::EmbedCallerInSQL = 1;
  my $cv = AnyEvent->condvar;

  my $sql;
  $db->delete ('foo', {id => 1},
               cb => sub { $sql = $_[0]->{last_sql}; $cv->send },
               source_name => 'ae');

  $cv->recv;

  is $sql,
      'DELETE FROM `foo` WHERE `id` = ? /* ae at '.__FILE__.
      ' line '.(__LINE__-6).' */';
} # _delete_embedded

sub _set_tz_embedded : Test(1) {
  my $db = new_db;
  $db->source (ae => {dsn => $db->source ('master')->{dsn}, anyevent => 1,
                      writable => 1});

  local $Dongry::Database::EmbedCallerInSQL = 1;
  my $cv = AnyEvent->condvar;

  my $sql;
  $db->set_tz ('',
               cb => sub { $sql = $_[0]->{last_sql}; $cv->send },
               source_name => 'ae');

  $cv->recv;

  is $sql,
      'SET time_zone = ? /* ae at '.__FILE__.
      ' line '.(__LINE__-6).' */';
} # _set_tz_embedded

sub _execute_embedded_line : Test(1) {
  my $db = new_db;
  $db->source (ae => {dsn => $db->source ('master')->{dsn}, anyevent => 1,
                      writable => 1});

  local $Dongry::Database::EmbedCallerInSQL = 1;
  my $cv = AnyEvent->condvar;
  my $sql;
#line 52 "foo/*bar*/baz/*hoge*/fuga"
  $db->execute ('show tables', undef,
                cb => sub { $sql = $_[0]->{last_sql}; $cv->send },
                source_name => 'ae');

  $cv->recv;

  is $sql,
      'show tables /* ae at foo/*bar* /baz/*hoge* /fuga line 54 */';
} # _execute_embedded_line

__PACKAGE__->runtests;

1;

=head1 LICENSE

Copyright 2012 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
