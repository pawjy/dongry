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

sub new_row (@) {
  return bless {@_}, 'Dongry::Table::Row';
} # new_row

sub _version : Test(1) {
  ok $Dongry::Table::Row::VERSION;
} # _version

sub _table_name : Test(1) {
  my $row = new_row table_name => 'foo';
  is $row->table_name, 'foo';
} # _table_name

sub _table_schema_none : Test(1) {
  my $db = Dongry::Database->new;
  my $row = new_row db => $db, table_name => 'foo';
  is $row->table_schema, undef;
} # _table_schema_none

sub _table_schema_none_for_table : Test(1) {
  my $db = Dongry::Database->new
      (schema => {bar => {abac => 1}});
  my $row = new_row db => $db, table_name => 'foo';
  is $row->table_schema, undef;
} # _table_schema_none_for_table

sub _table_schema_found : Test(1) {
  my $db = Dongry::Database->new
      (schema => {bar => {abac => 1}});
  my $row = new_row db => $db, table_name => 'bar';
  eq_or_diff $row->table_schema, {abac => 1};
} # _table_schema_found

sub _flags_none : Test(2) {
  my $db = Dongry::Database->new;
  my $row = new_row db => $db, table_name => 'hoge';

  is $row->flags->{hoge}, undef;
  
  $row->flags->{hoge} = 1253;
  is $row->flags->{hoge}, 1253;
} # _flags_none

sub _flags_specified : Test(2) {
  my $db = Dongry::Database->new;
  my $row = new_row db => $db, table_name => 'hoge', flags => {foo => 124};

  is $row->flags->{foo}, 124;
  
  $row->flags->{foo} = 1253;
  is $row->flags->{foo}, 1253;
} # _flags_specified

sub _debug_info : Test(1) {
  my $db = new_db schema => {foo => {}};
  $db->execute ('create table foo (id int)');
  $db->execute ('insert into foo (id) values (2)');
  my $row = $db->table ('foo')->find ({id => 2});
  is $row->debug_info, '{Row: foo}';
} # _debug_info

sub _debug_info_pk : Test(1) {
  my $db = new_db schema => {foo => {primary_keys => ['id']}};
  $db->execute ('create table foo (id int)');
  $db->execute ('insert into foo (id) values (2)');
  my $row = $db->table ('foo')->find ({id => 2});
  is $row->debug_info, '{Row: foo: id = 2}';
} # _debug_info_pk

sub _debug_info_pks : Test(1) {
  my $db = new_db schema => {foo => {primary_keys => ['id', 'id2']}};
  $db->execute ('create table foo (id int, id2 int)');
  $db->execute ('insert into foo (id, id2) values (2, 3)');
  my $row = $db->table ('foo')->find ({id => 2, id2 => 3});
  ok $row->debug_info;
} # _debug_info_pks

sub _debug_info_pk_none : Test(1) {
  my $db = new_db schema => {foo => {primary_keys => ['id']}};
  $db->execute ('create table foo (id int)');
  $db->execute ('insert into foo (id) values (2)');
  my $row = $db->table ('foo')->find ({id => 2}, fields => {-count => undef});
  is $row->debug_info, '{Row: foo}';
} # _debug_info_pk_none

__PACKAGE__->runtests;

1;

=head1 LICENSE

Copyright 2011 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
