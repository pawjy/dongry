package test::Dongry::Table::basic;
use strict;
use warnings;
use Path::Class;
use lib file (__FILE__)->dir->parent->subdir ('lib')->stringify;
use Test::Dongry;
use base qw(Test::Class);
use Dongry::Database;
use Dongry::Table;
use Encode;

sub _version : Test(1) {
  ok $Dongry::Table::VERSION;
} # _version

sub _new : Test(3) {
  my $db = Dongry::Database->new;
  my $table = Dongry::Table->new
      (db => $db, name => 'hoge');
  isa_ok $table, 'Dongry::Table';
  is $table->{db}, $db;
  is $table->name, 'hoge';
} # _new

sub _schema_none : Test(1) {
  my $db = Dongry::Database->new;
  my $table = Dongry::Table->new (db => $db, name => 'foo');
  eq_or_diff $table->schema, undef;
} # _schema_none

sub _schema_none_for_table : Test(1) {
  my $db = Dongry::Database->new
      (schema => {bar => {a => 2}});
  my $table = Dongry::Table->new (db => $db, name => 'foo');
  eq_or_diff $table->schema, undef;
} # _schema_none_for_table

sub _schema_found : Test(1) {
  my $db = Dongry::Database->new
      (schema => {bar => {a => 2}});
  my $table = Dongry::Table->new (db => $db, name => 'bar');
  eq_or_diff $table->schema, {a => 2};
} # _schema_found

__PACKAGE__->runtests;

1;

=head1 LICENSE

Copyright 2011 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
