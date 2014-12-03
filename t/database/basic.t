package test::Dongry::Database::basic;
use strict;
use warnings;
use Path::Class;
use lib file (__FILE__)->dir->parent->subdir ('lib')->stringify;
use Test::Dongry;
use base qw(Test::Class);
use Dongry::Database;

sub _version : Test(8) {
  ok $Dongry::Database::VERSION;
  ok $Dongry::Database::Executed::VERSION;
  ok $Dongry::Database::Executed::Inserted::VERSION;
  ok $Dongry::Database::Executed::NoResult::VERSION;
  ok $Dongry::Database::Transaction::VERSION;
  ok $Dongry::Database::ForceSource::VERSION;
  ok $Dongry::Database::BrokenConnection::VERSION;
  ok $Dongry::Database::Registry::VERSION;
} # _version

sub _inheritance : Test(1) {
  ok +Dongry::Database::Executed::Inserted->isa ('Dongry::Database::Executed');
} # _inheritance

sub _not_imported : Test(4) {
  dies_ok { Dongry::Database->quote };
  dies_ok { Dongry::Database->fields };
  dies_ok { Dongry::Database->where };
  dies_ok { Dongry::Database->order };
} # _not_imported

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
  dies_here_ok { Dongry::Database->load ('notfound') };
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

sub _load_empty_def : Test(5) {
  local $Dongry::Database::Registry->{test1} = {};
  my $db = Dongry::Database->load ('test1');
  ng $db->{sources};
  ng $db->{onerror};
  ng $db->{onconnect};
  ng $db->{schema};
  ng $db->{table_name_normalizer};
} # _load_empty_def

sub _load_static_def : Test(5) {
  local $Dongry::Database::Registry->{test2}
      = {sources => {foo => {dsn => 123}},
         onerror => 123,
         onconnect => 154,
         schema => {hoge => {foo => 5}},
         table_name_normalizer => sub { 12 }};
  my $db = Dongry::Database->load ('test2');
  eq_or_diff $db->{sources}, {foo => {dsn => 123}};
  is $db->{onerror}, 123;
  is $db->{onconnect}, 154;
  eq_or_diff $db->{schema}, {hoge => {foo => 5}};
  is $db->{table_name_normalizer}->(), 12;
} # _load_static_def

sub _load_dynamic_def : Test(5) {
  local $Dongry::Database::Registry->{test3}
      = {get_sources => sub { +{foo => {dsn => 123}} },
         get_onerror => sub { 123 },
         get_onconnect => sub { 154 },
         get_schema => sub { +{hoge => {foo => 5}}},
         get_table_name_normalizer => sub { sub { 120 } }};
  my $db = Dongry::Database->load ('test3');
  eq_or_diff $db->{sources}, {foo => {dsn => 123}};
  is $db->{onerror}, 123;
  is $db->{onconnect}, 154;
  eq_or_diff $db->{schema}, {hoge => {foo => 5}};
  is $db->{table_name_normalizer}->(), 120;
} # _load_dynamic_def

sub _create_registry : Test(3) {
  my $reg = Dongry::Database->create_registry;
  isa_ok $reg, 'Dongry::Database::Registry';

  my $reg2 = Dongry::Database->create_registry;
  isa_ok $reg2, 'Dongry::Database::Registry';

  isnt $reg2, $reg;
} # _create_registry

sub _create_registry_load_none : Test(3) {
  my $reg = Dongry::Database->create_registry;

  dies_here_ok {
      $reg->load;
  };
  dies_here_ok {
      $reg->load(0);
  };
  dies_here_ok {
      $reg->load('hoge');
  };
} # _create_registry_load_none

sub _create_registry_load_found : Test(4) {
  my $reg = Dongry::Database->create_registry;
  $reg->{Registry}->{hoge} = {
    sources => {master => {dsn => 'hoge'}},
  };

  my $db = $reg->load ('hoge');
  eq_or_diff $db->source('master'), {dsn => 'hoge'};

  my $db2 = $reg->load ('hoge');
  is $db2, $db;

  dies_here_ok {
      $reg->load ('HOGE');
  };

  dies_here_ok {
      Dongry::Database->load ('hoge');
  };
} # _create_registry_load_found

sub _create_registry_load_found_2 : Test(4) {
  local $Dongry::Database::Registry->{hoge} = {
    sources => {master => {dsn => 'fuga'}},
  };

  my $reg = Dongry::Database->create_registry;
  $reg->{Registry}->{hoge} = {
    sources => {master => {dsn => 'hoge'}},
  };

  my $db = $reg->load ('hoge');
  eq_or_diff $db->source('master'), {dsn => 'hoge'};

  my $db2 = $reg->load ('hoge');
  is $db2, $db;

  my $db3 = Dongry::Database->load ('hoge');
  eq_or_diff $db3->source('master'), {dsn => 'fuga'};
} # _create_registry_load_found_2

sub _debug_info : Test(1) {
  my $db = Dongry::Database->new;
  is $db->debug_info, '{DB: }';
} # _debug_info

sub _debug_info_2 : Test(1) {
  my $db = Dongry::Database->new
      (sources => {default => {dsn => 'fuga'}});
  is $db->debug_info, '{DB: default = fuga}';
} # _debug_info_2

sub _debug_info_3 : Test(1) {
  my $db = Dongry::Database->new
      (sources => {default => {dsn => 'fuga', label => 'f'}});
  is $db->debug_info, '{DB: default = f}';
} # _debug_info_3

sub _debug_info_4 : Test(1) {
  my $db = Dongry::Database->new
      (sources => {master => {dsn => 'hoge'},
                   default => {dsn => 'fuga', label => 'f'}});
  #warn $db->debug_info;
  ok $db->debug_info;
} # _debug_info_4

sub _executed_debug_info : Test(1) {
  my $db = new_db;
  my $result = $db->execute ('show tables');
  is $result->debug_info, '{DBExecuted: }';
} # _executed_debug_info

sub _executed_debug_info_2 : Test(1) {
  my $db = new_db;
  $db->execute ('create table foo (id int)');
  my $result = $db->select ('foo', {id => 0});
  is $result->debug_info, '{DBExecuted: table_name = foo}';
} # _executed_debug_info_2

sub _executed_debug_info_3 : Test(1) {
  my $db = new_db;
  $db->execute ('create table foo (id int)');
  my $result = $db->insert ('foo', [{id => 0}]);
  is $result->debug_info, '{DBExecuted: table_name = foo}';
} # _executed_debug_info_3

sub _executed_stringify_1 : Test(2) {
  my $db = new_db;
  $db->execute ('create table foo (id int)');
  my $result = $db->insert ('foo', [{id => 0}]);
  ok !!$result;
  ok ''.$result;
} # _executed_stringify_1

sub _transaction_debug_info : Test(1) {
  my $db = new_db;
  my $transaction = $db->transaction;
  is $transaction->debug_info, '{DBTransaction}';
  $transaction->commit;
} # _transaction_debug_info

sub _bare_sql_fragment_class : Test(1) {
  my $sql = Dongry::Database->bare_sql_fragment ('abc def');
  is $$sql, 'abc def';
} # _bare_sql_fragment_class

sub _bare_sql_fragment_instance : Test(1) {
  my $sql = Dongry::Database->new->bare_sql_fragment ('abc def');
  is $$sql, 'abc def';
} # _bare_sql_fragment_instance

__PACKAGE__->runtests;

1;

=head1 LICENSE

Copyright 2011-2012 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
