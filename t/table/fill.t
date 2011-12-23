package test::Dongry::Table::fill;
use strict;
use warnings;
use Path::Class;
use lib file (__FILE__)->dir->parent->subdir ('lib')->stringify;
use Test::Dongry;
use base qw(Test::Class);
use Test::MoreMore::Mock;
BEGIN { $DBIx::ShowSQL::WARN //= 0 };
use DBIx::ShowSQL;
use Dongry::Database;
use Dongry::Type::DateTime;
use Encode;

sub new_db (%) {
  my %args = @_;
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}},
       schema => $args{schema});
  for my $name (keys %{$args{schema} || {}}) {
    if ($args{schema}->{$name}->{_create}) {
      $db->execute ($args{schema}->{$name}->{_create});
    }
  }
  return $db;
} # new_db

# ------ |fill_related_rows| ------

sub _fill_related_rows_simple_1 : Test(3) {
  my $schema = {
    table1 => {
      _create => 'create table table1 (id int)',
    },
  };
  my $db = new_db schema => $schema;
  my $table = $db->table ('table1');

  $table->create ({id => 124});
  $table->create ({id => 12345});

  my $mock1 = Test::MoreMore::Mock->new (related_id => 12345);

  local $DBIx::ShowSQL::SQLCount = 0;
  $table->fill_related_rows
      ([$mock1] => {related_id => 'id'} => 'related_row');
  is $DBIx::ShowSQL::SQLCount, 1;

  isa_ok $mock1->related_row, 'Dongry::Table::Row';
  is $mock1->related_row->get ('id'), 12345;
} # _fill_related_rows_simple_1

sub _fill_related_rows_simple_1_replace : Test(3) {
  my $schema = {
    table1 => {
      _create => 'create table table1 (id int)',
    },
  };
  my $db = new_db schema => $schema;
  my $table = $db->table ('table1');

  $table->create ({id => 124});
  $table->create ({id => 12345});

  my $mock1 = Test::MoreMore::Mock->new (related_id => 12345);
  $mock1->related_row ('hogehoge');

  local $DBIx::ShowSQL::SQLCount = 0;
  $table->fill_related_rows
      ([$mock1] => {related_id => 'id'} => 'related_row');
  is $DBIx::ShowSQL::SQLCount, 1;

  isa_ok $mock1->related_row, 'Dongry::Table::Row';
  is $mock1->related_row->get ('id'), 12345;
} # _fill_related_rows_simple_1_replace

sub _fill_related_rows_simple_not_found : Test(2) {
  my $schema = {
    table1 => {
      _create => 'create table table1 (id int)',
    },
  };
  my $db = new_db schema => $schema;
  my $table = $db->table ('table1');

  $table->create ({id => 124});

  my $mock1 = Test::MoreMore::Mock->new (related_id => 12345);
  $mock1->related_row ('hogehoge');

  local $DBIx::ShowSQL::SQLCount = 0;
  $table->fill_related_rows
      ([$mock1] => {related_id => 'id'} => 'related_row');
  is $DBIx::ShowSQL::SQLCount, 1;

  is $mock1->related_row, undef;
} # _fill_related_rows_simple_not_found

sub _fill_related_rows_multiple : Test(3) {
  my $schema = {
    table1 => {
      _create => 'create table table1 (id int)',
    },
  };
  my $db = new_db schema => $schema;
  my $table = $db->table ('table1');

  $table->create ({id => 124});
  $table->create ({id => 512});

  my $mock1 = Test::MoreMore::Mock->new (related_id => 124);
  my $mock2 = Test::MoreMore::Mock->new (related_id => 512);

  local $DBIx::ShowSQL::SQLCount = 0;
  $table->fill_related_rows
      ([$mock1, $mock2] => {related_id => 'id'} => 'related_row');
  is $DBIx::ShowSQL::SQLCount, 1;

  is $mock1->related_row->get ('id'), 124;
  is $mock2->related_row->get ('id'), 512;
} # _fill_related_rows_multiple

sub _fill_related_rows_zero_list : Test(1) {
  my $schema = {
    table1 => {
      _create => 'create table table1 (id int)',
    },
  };
  my $db = new_db schema => $schema;
  my $table = $db->table ('table1');

  $table->create ({id => 124});
  $table->create ({id => 512});

  local $DBIx::ShowSQL::SQLCount = 0;
  $table->fill_related_rows
      ([] => {related_id => 'id'} => 'related_row');
  is $DBIx::ShowSQL::SQLCount, 0;
} # _fill_related_rows_zero_list

sub _fill_related_rows_multiple_results : Test(2) {
  my $schema = {
    table1 => {
      _create => 'create table table1 (id int)',
    },
  };
  my $db = new_db schema => $schema;
  my $table = $db->table ('table1');

  $table->create ({id => 124});
  $table->create ({id => 124});

  my $mock1 = Test::MoreMore::Mock->new (related_id => 124);

  local $DBIx::ShowSQL::SQLCount = 0;
  $table->fill_related_rows
      ([$mock1] => {related_id => 'id'} => 'related_row');
  is $DBIx::ShowSQL::SQLCount, 1;

  is $mock1->related_row->get ('id'), 124;
} # _fill_related_rows_multiple_results

sub _fill_related_rows_multiple_same_items : Test(4) {
  my $schema = {
    table1 => {
      _create => 'create table table1 (id int)',
    },
  };
  my $db = new_db schema => $schema;
  my $table = $db->table ('table1');

  $table->create ({id => 124});

  my $mock1 = Test::MoreMore::Mock->new (related_id => 124);
  my $mock2 = Test::MoreMore::Mock->new (related_id => 124);
  my $mock3 = Test::MoreMore::Mock->new (related_id => 124);

  local $DBIx::ShowSQL::SQLCount = 0;
  $table->fill_related_rows
      ([$mock1, $mock2, $mock3] => {related_id => 'id'} => 'related_row');
  is $DBIx::ShowSQL::SQLCount, 1;

  is $mock1->related_row->get ('id'), 124;
  is $mock2->related_row->get ('id'), 124;
  is $mock3->related_row->get ('id'), 124;
} # _fill_related_rows_multiple_same_items

sub _fill_related_rows_multiple_found_1 : Test(4) {
  my $schema = {
    table1 => {
      _create => 'create table table1 (id int)',
    },
  };
  my $db = new_db schema => $schema;
  my $table = $db->table ('table1');

  $table->create ({id => 124});
  $table->create ({id => 12345});

  my $mock1 = Test::MoreMore::Mock->new (related_id => 12345);

  local $DBIx::ShowSQL::SQLCount = 0;
  $table->fill_related_rows
      ([$mock1] => {related_id => 'id'} => 'related_rows', multiple => 1);
  is $DBIx::ShowSQL::SQLCount, 1;

  isa_list_n_ok $mock1->related_rows, 1;
  isa_ok $mock1->related_rows->[0], 'Dongry::Table::Row';
  is $mock1->related_rows->[0]->get ('id'), 12345;
} # _fill_related_rows_multiple_found_1

sub _fill_related_rows_multiple_found_2 : Test(6) {
  my $schema = {
    table1 => {
      _create => 'create table table1 (id int)',
    },
  };
  my $db = new_db schema => $schema;
  my $table = $db->table ('table1');

  $table->create ({id => 12345});
  $table->create ({id => 12345});

  my $mock1 = Test::MoreMore::Mock->new (related_id => 12345);

  local $DBIx::ShowSQL::SQLCount = 0;
  $table->fill_related_rows
      ([$mock1] => {related_id => 'id'} => 'related_rows', multiple => 1);
  is $DBIx::ShowSQL::SQLCount, 1;

  isa_list_n_ok $mock1->related_rows, 2;
  isa_ok $mock1->related_rows->[0], 'Dongry::Table::Row';
  is $mock1->related_rows->[0]->get ('id'), 12345;
  isa_ok $mock1->related_rows->[1], 'Dongry::Table::Row';
  is $mock1->related_rows->[1]->get ('id'), 12345;
} # _fill_related_rows_multiple_found_2

sub _fill_related_rows_multiple_found_0 : Test(2) {
  my $schema = {
    table1 => {
      _create => 'create table table1 (id int)',
    },
  };
  my $db = new_db schema => $schema;
  my $table = $db->table ('table1');

  my $mock1 = Test::MoreMore::Mock->new (related_id => 12345);

  local $DBIx::ShowSQL::SQLCount = 0;
  $table->fill_related_rows
      ([$mock1] => {related_id => 'id'} => 'related_rows', multiple => 1);
  is $DBIx::ShowSQL::SQLCount, 1;

  isa_list_n_ok $mock1->related_rows, 0;
} # _fill_related_rows_multiple_found_0

sub _fill_related_rows_multiple_found_multiples : Test(7) {
  my $schema = {
    table1 => {
      _create => 'create table table1 (id int)',
    },
  };
  my $db = new_db schema => $schema;
  my $table = $db->table ('table1');

  $table->create ({id => 12345});
  $table->create ({id => 12345});
  $table->create ({id => 12341});

  my $mock1 = Test::MoreMore::Mock->new (related_id => 12345);
  my $mock2 = Test::MoreMore::Mock->new (related_id => 12341);
  my $mock3 = Test::MoreMore::Mock->new (related_id => 12347);

  local $DBIx::ShowSQL::SQLCount = 0;
  $table->fill_related_rows
      ([$mock1, $mock2, $mock3] => {related_id => 'id'} => 'related_rows',
       multiple => 1);
  is $DBIx::ShowSQL::SQLCount, 1;

  isa_list_n_ok $mock1->related_rows, 2;
  is $mock1->related_rows->[0]->get ('id'), 12345;
  is $mock1->related_rows->[1]->get ('id'), 12345;

  isa_list_n_ok $mock2->related_rows, 1;
  is $mock2->related_rows->[0]->get ('id'), 12341;

  isa_list_n_ok $mock3->related_rows, 0;
} # _fill_related_rows_multiple_found_multiples

__PACKAGE__->runtests;

1;

=head1 LICENSE

Copyright 2011 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
