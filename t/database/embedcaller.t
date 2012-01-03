package test::Dongry::Database::embedcaller;
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

sub _execute_embedded : Test(1) {
  my $db = new_db;

  local $Dongry::Database::EmbedCallerInSQL = 1;
  $db->execute ('show tables');

  is $db->{last_sql},
      'show tables /* default at '.__FILE__.' line '.(__LINE__-3).' */';
} # _execute_embedded

sub _select_embedded : Test(1) {
  my $db = new_db;
  $db->execute ('create table foo (id int)');

  local $Dongry::Database::EmbedCallerInSQL = 1;
  $db->select ('foo', {id => 1});

  is $db->{last_sql},
      'SELECT * FROM `foo` WHERE `id` = ? /* default at '.__FILE__.
      ' line '.(__LINE__-4).' */';
} # _select_embedded

sub _insert_embedded : Test(1) {
  my $db = new_db;
  $db->execute ('create table foo (id int)');

  local $Dongry::Database::EmbedCallerInSQL = 1;
  $db->insert ('foo', [{id => 1}]);

  is $db->{last_sql},
      'INSERT INTO `foo` (`id`) VALUES (?) /* master at '.__FILE__.
      ' line '.(__LINE__-4).' */';
} # _insert_embedded

sub _execute_embedded_line : Test(1) {
  my $db = new_db;

  local $Dongry::Database::EmbedCallerInSQL = 1;
#line 52 "foo/*bar*/baz/*hoge*/fuga"
  $db->execute ('show tables');

  is $db->{last_sql},
      'show tables /* default at foo/*bar* /baz/*hoge* /fuga line 52 */';
} # _execute_embedded_line

__PACKAGE__->runtests;

1;

=head1 LICENSE

Copyright 2012 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
