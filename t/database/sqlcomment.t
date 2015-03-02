package test::Dongry::Database::sqlcomment;
use strict;
use warnings;
use Path::Class;
use lib file (__FILE__)->dir->parent->subdir ('lib')->stringify;
use Test::Dongry;
use base qw(Test::Class);
use Dongry::Database;

sub _execute_plain : Test(1) {
  my $db = new_db;

  $db->execute ('show tables');

  is $db->{last_sql}, 'show tables';
} # _execute_plain

sub _execute_commented : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'createtable';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, sql_comment => '*/ hello-dongry /*'}});

  $db->execute ('show tables');

  is $db->{last_sql},
      'show tables /* * / hello-dongry /* */';
} # _execute_commented

sub _execute_commented_0 : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'createtable';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, sql_comment => '0'}});

  $db->execute ('show tables');

  is $db->{last_sql},
      'show tables /* 0 */';
} # _execute_commented_0

sub _execute_commented_empty : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'createtable';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, sql_comment => ''}});

  $db->execute ('show tables');

  is $db->{last_sql},
      'show tables /*  */';
} # _execute_commented_empty

sub _select_commented : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'createtable';
  my $db = Dongry::Database->new
      (sources => {master  => {dsn => $dsn, writable => 1, sql_comment => '*/ hello-dongry /*'},
                   default => {dsn => $dsn, sql_comment => '*/ hello-dongry /*'}});
  $db->execute ('create table foo (id int)');

  $db->select ('foo', {id => 1});

  is $db->{last_sql},
      'SELECT * FROM `foo` WHERE `id` = ? /* * / hello-dongry /* */';
} # _select_commented

sub _insert_commented : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'createtable';
  my $db = Dongry::Database->new
      (sources => {master  => {dsn => $dsn, writable => 1, sql_comment => '*/ hello-dongry /*'},
                   default => {dsn => $dsn, sql_comment => '*/ hello-dongry /*'}});
  $db->execute ('create table foo (id int)');

  $db->insert ('foo', [{id => 1}]);

  is $db->{last_sql},
      'INSERT INTO `foo` (`id`) VALUES (?) /* * / hello-dongry /* */';
} # _insert_commented

__PACKAGE__->runtests;

1;

=head1 LICENSE

Copyright 2012 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
