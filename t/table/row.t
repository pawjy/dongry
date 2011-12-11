package test::Dongry::Table::Row;
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
  ok $Dongry::Table::Row::VERSION;
} # _version

sub _new_row : Test(3) {
  my $row = Dongry::Table->new_row
      (hoge => 1, foo => 'fug');
  isa_ok $row, 'Dongry::Table::Row';
  is $row->{hoge}, 1;
  is $row->{foo}, 'fug';
} # _new_row

sub _table_name : Test(1) {
  my $row = Dongry::Table->new_row (table_name => 'foo');
  is $row->table_name, 'foo';
} # _table_name

sub _table_schema_none : Test(1) {
  my $db = Dongry::Database->new;
  my $row = Dongry::Table->new_row (db => $db, table_name => 'foo');
  is $row->table_schema, undef;
} # _table_schema_none

sub _table_schema_none_for_table : Test(1) {
  my $db = Dongry::Database->new
      (schema => {bar => {abac => 1}});
  my $row = Dongry::Table->new_row (db => $db, table_name => 'foo');
  is $row->table_schema, undef;
} # _table_schema_none_for_table

sub _table_schema_found : Test(1) {
  my $db = Dongry::Database->new
      (schema => {bar => {abac => 1}});
  my $row = Dongry::Table->new_row (db => $db, table_name => 'bar');
  eq_or_diff $row->table_schema, {abac => 1};
} # _table_schema_found

__PACKAGE__->runtests;

1;

=head1 LICENSE

Copyright 2011 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
