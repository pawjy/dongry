package test::Dongry::Database::basic;
use strict;
use warnings;
use Path::Class;
use lib file (__FILE__)->dir->parent->subdir ('lib')->stringify;
use Test::Dongry;
use base qw(Test::Class);
use Dongry::Database;

sub _version : Test(4) {
  ok $Dongry::Database::VERSION;
  ok $Dongry::Database::Executed::VERSION;
  ok $Dongry::Database::Executed::Inserted::VERSION;
  ok $Dongry::Database::Transaction::VERSION;
} # _version

# ------ Instantiation ------

sub _new_empty : Test(1) {
  my $db = Dongry::Database->new;
  isa_ok $db, 'Dongry::Database';
} # _new_empty

sub _new_with_args : Test(2) {
  my $db = Dongry::Database->new (sources => {hoge => {dsn => 123}});
  isa_ok $db, 'Dongry::Database';
  is $db->source ('hoge')->{dsn}, 123;
} # _new_with_args

sub _load_not_defined : Test(1) {
  dies_ok { Dongry::Database->load ('notfound') };
} # _load_not_defined

sub _load_found : Test(6) {
  local $Dongry::Database::Registry->{hoge1}
      = {sources => {hoge => {dsn => 123}}};

  my $db = Dongry::Database->load ('hoge1');
  isa_ok $db, 'Dongry::Database';
  is $db->source ('hoge')->{dsn}, 123;

  my $db2 = Dongry::Database->load ('hoge1');
  is $db2, $db;

  local $Dongry::Database::Instances = {};
  my $db3 = Dongry::Database->load ('hoge1');
  isnt $db3, $db;
  isa_ok $db3, 'Dongry::Database';
  is $db3->source ('hoge')->{dsn}, 123;
} # _load_found

sub _load_empty_def : Test(4) {
  local $Dongry::Database::Registry->{test1} = {};
  my $db = Dongry::Database->load ('test1');
  ng $db->{sources};
  ng $db->{onerror};
  ng $db->{onconnect};
  ng $db->{schema};
} # _load_empty_def

sub _load_static_def : Test(4) {
  local $Dongry::Database::Registry->{test2}
      = {sources => {foo => {dsn => 123}},
         onerror => 123,
         onconnect => 154,
         schema => {hoge => {foo => 5}}};
  my $db = Dongry::Database->load ('test2');
  eq_or_diff $db->{sources}, {foo => {dsn => 123}};
  is $db->{onerror}, 123;
  is $db->{onconnect}, 154;
  eq_or_diff $db->{schema}, {hoge => {foo => 5}};
} # _load_static_def

sub _load_dynamic_def : Test(4) {
  local $Dongry::Database::Registry->{test2}
      = {get_sources => sub { +{foo => {dsn => 123}} },
         get_onerror => sub { 123 },
         get_onconnect => sub { 154 },
         get_schema => sub { +{hoge => {foo => 5}}} };
  my $db = Dongry::Database->load ('test2');
  eq_or_diff $db->{sources}, {foo => {dsn => 123}};
  is $db->{onerror}, 123;
  is $db->{onconnect}, 154;
  eq_or_diff $db->{schema}, {hoge => {foo => 5}};
} # _load_dynamic_def

__PACKAGE__->runtests;

1;

=head1 LICENSE

Copyright 2011 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut