package test::Dongry::Query;
use strict;
use warnings;
use Path::Class;
use lib file (__FILE__)->dir->parent->subdir ('lib')->stringify;
use Test::Dongry;
use base qw(Test::Class);
use Dongry::Database;
use Dongry::Query;

sub _version : Test(1) {
  ok $Dongry::Query::VERSION;
} # _version

sub _new : Test(3) {
  my $q = Dongry::Query->new (table_name => 'hoge', order => [foo => -1]);
  isa_ok $q, 'Dongry::Query';
  is $q->table_name, 'hoge';
  eq_or_diff $q->order, [foo => -1];
} # _new

sub _db : Test(2) {
  my $db = Dongry::Database->new;
  my $q = Dongry::Query->new (db => $db);
  is $q->db, $db;

  my $db2 = Dongry::Database->new;
  $q->db ($db2);
  is $q->db, $db2;
} # _db

sub _table : Test(4) {
  my $db = Dongry::Database->new;
  my $q = Dongry::Query->new (db => $db, table_name => 'hoge');
  
  is $q->table_name, 'hoge';
  isa_ok $q->table, 'Dongry::Table';
  is $q->table->table_name, 'hoge';
  is $q->table, $q->table;
} # _table

sub _fields : Test(4) {
  my $q = Dongry::Query->new;
  is $q->fields, undef;

  $q->fields ({hoge => {foo => 1}});
  eq_or_diff $q->fields, {hoge => {foo => 1}};
  eq_or_diff $q->fields, {hoge => {foo => 1}};
  
  $q->fields (undef);
  is $q->fields, undef;
} # _fields

sub _where : Test(4) {
  my $q = Dongry::Query->new;
  is $q->where, undef;

  $q->where ({hoge => {foo => 1}});
  eq_or_diff $q->where, {hoge => {foo => 1}};
  eq_or_diff $q->where, {hoge => {foo => 1}};
  
  $q->where (undef);
  is $q->where, undef;
} # _where

sub _order : Test(4) {
  my $q = Dongry::Query->new;
  is $q->order, undef;

  $q->order ({hoge => {foo => 1}});
  eq_or_diff $q->order, {hoge => {foo => 1}};
  eq_or_diff $q->order, {hoge => {foo => 1}};
  
  $q->order (undef);
  is $q->order, undef;
} # _order

sub _group : Test(4) {
  my $q = Dongry::Query->new;
  is $q->group, undef;

  $q->group ({hoge => {foo => 1}});
  eq_or_diff $q->group, {hoge => {foo => 1}};
  eq_or_diff $q->group, {hoge => {foo => 1}};
  
  $q->group (undef);
  is $q->group, undef;
} # _group

sub _item_list_filter : Test(3) {
  my $q = Dongry::Query->new;

  eq_or_diff $q->item_list_filter
      (List::Rubyish->new ([{foo => 1243, bar => 12}, {foo => 12}]))->to_a,
      [{foo => 1243, bar => 12}, {foo => 12}];

  $q->{item_list_filter} = sub { is $_[0], $q; $_[1]->map (sub { $_ + 12 }) };
  eq_or_diff $q->item_list_filter (List::Rubyish->new ([124, 66]))->to_a,
       [124 + 12, 66 + 12];
} # _fields

__PACKAGE__->runtests;

1;

=head1 LICENSE

Copyright 2011 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
